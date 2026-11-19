using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Banking.Domain;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Banking.Application;

public sealed class TransactionProcessingOptions
{
    /// <summary>Max instructions durably committed in one batch (multi-row UPSERT).</summary>
    public int MaxBatchSize { get; set; } = 512;

    /// <summary>How long the aggregator waits to fill a batch. Bounds added latency.</summary>
    public TimeSpan BatchLinger { get; set; } = TimeSpan.FromMilliseconds(2);

    /// <summary>Bound on in-flight requests; excess is shed with backpressure (API times out -> 503).</summary>
    public int MaxQueueDepth { get; set; } = 100_000;

    /// <summary>Optimistic-concurrency retry attempts per conflicting batch.</summary>
    public int MaxRetries { get; set; } = 3;
}

/// <summary>
/// High-throughput transaction pipeline:
///
///   HTTP request --> bounded Channel&lt;T&gt; (backpressure)
///       --> Aggregator loop: drains up to MaxBatchSize items OR flushes after BatchLinger
///             1. Idempotency dedupe (in-batch HashSet + atomic store-side reserve)
///             2. Load touched accounts in ONE round trip
///             3. Apply domain rules in deterministic order (deadlock-free)
///             4. Persist accounts + ledger entries in ONE batched durable commit
///             5. Complete each caller's TaskCompletionSource exactly once
///
/// Each queued item owns exactly one TCS, so completion is race-free by construction.
/// Transfers are single items producing two ledger legs — never split across batches.
/// </summary>
public sealed class TransactionProcessor : ITransactionProcessor, IAsyncDisposable
{
    private readonly Channel<Item> _channel;
    private readonly TransactionProcessingOptions _options;
    private readonly ITransactionStore _store;
    private readonly IEventSink _events;
    private readonly ILogger<TransactionProcessor> _logger;
    private readonly Task _aggregatorLoop;
    private long _processed, _rejected, _deduped, _queued;

    public TransactionProcessor(
        ITransactionStore store,
        IEventSink events,
        IOptions<TransactionProcessingOptions> options,
        ILogger<TransactionProcessor> logger)
    {
        _store = store;
        _events = events;
        _options = Validate(options.Value);
        _logger = logger;

        _channel = Channel.CreateBounded<Item>(new BoundedChannelOptions(_options.MaxQueueDepth)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            AllowSynchronousContinuations = false
        });

        _aggregatorLoop = Task.Run(AggregateLoopAsync);
    }

    private static TransactionProcessingOptions Validate(TransactionProcessingOptions o)
    {
        if (o.MaxBatchSize is < 1 or > 10_000) throw new ArgumentOutOfRangeException(nameof(o.MaxBatchSize));
        if (o.MaxQueueDepth <= 0) throw new ArgumentOutOfRangeException(nameof(o.MaxQueueDepth));
        return o;
    }

    private sealed class Item
    {
        public required TransactionInstruction Instruction { get; init; }
        public required TaskCompletionSource<ProcessResult> Tcs { get; init; }
        public Guid CorrelationId { get; init; }
        public bool Applied { get; set; }
        public ProcessResult Result; // populated during apply phase
    }

    public ValueTask<ProcessResult> SubmitAsync(TransactionInstruction instruction, CancellationToken ct = default)
    {
        if (instruction.AmountMinorUnits <= 0)
            return ValueTask.FromResult(ProcessResult.Fail("Amount must be positive."));
        if (string.IsNullOrEmpty(instruction.IdempotencyKey))
            return ValueTask.FromResult(ProcessResult.Fail("Idempotency-Key is required."));

        var tcs = new TaskCompletionSource<ProcessResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = new Item
        {
            Instruction = instruction,
            Tcs = tcs,
            CorrelationId = Guid.NewGuid()
        };

        Interlocked.Increment(ref _queued);
        if (_channel.Writer.TryWrite(item))
            return new ValueTask<ProcessResult>(tcs.Task);

        // Queue full: slow path with backpressure & cancellation.
        return EnqueueSlowAsync(item, ct);
    }

    private async ValueTask<ProcessResult> EnqueueSlowAsync(Item item, CancellationToken ct)
    {
        try
        {
            await _channel.Writer.WriteAsync(item, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Decrement(ref _queued);
            return ProcessResult.Fail("Request cancelled while queued (backpressure).");
        }
        catch (ChannelClosedException)
        {
            Interlocked.Decrement(ref _queued);
            return ProcessResult.Fail("Processor shutting down.");
        }
        return await item.Tcs.Task.ConfigureAwait(false);
    }

    public async IAsyncEnumerable<ProcessResult> SubmitBatchAsync(
        IReadOnlyList<TransactionInstruction> instructions,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var tasks = new ValueTask<ProcessResult>[instructions.Count];
        for (int i = 0; i < instructions.Count; i++)
            tasks[i] = SubmitAsync(instructions[i], ct);

        foreach (var t in tasks)
            yield return await t.ConfigureAwait(false);
    }

    private async Task AggregateLoopAsync()
    {
        var reader = _channel.Reader;
        var batch = new List<Item>(_options.MaxBatchSize);

        while (await reader.WaitToReadAsync().ConfigureAwait(false))
        {
            batch.Clear();
            using var linger = new CancellationTokenSource(_options.BatchLinger);

            try
            {
                while (batch.Count < _options.MaxBatchSize &&
                       await reader.WaitToReadAsync(linger.Token).ConfigureAwait(false))
                {
                    while (batch.Count < _options.MaxBatchSize && reader.TryRead(out var item))
                        batch.Add(item);
                }
            }
            catch (OperationCanceledException) { /* linger elapsed -> flush partial batch */ }

            if (batch.Count > 0)
            {
                Interlocked.Add(ref _queued, -batch.Count);
                await ProcessBatchSafeAsync(batch.ToArray()).ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessBatchSafeAsync(Item[] batch)
    {
        try
        {
            await ProcessBatchAsync(batch).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch failed catastrophically; failing {Count} requests.", batch.Length);
            foreach (var item in batch)
                item.Tcs.TrySetResult(ProcessResult.Fail("Internal processing error."));
        }
    }

    private async Task ProcessBatchAsync(Item[] batch)
    {
        // ---- 1. In-batch dedupe, then atomic store-side reservation ----
        var fresh = new List<Item>(batch.Length);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in batch)
        {
            if (!seen.Add(item.Instruction.IdempotencyKey))
            {
                Interlocked.Increment(ref _deduped);
                item.Tcs.TrySetResult(ProcessResult.Fail("Duplicate request already in flight."));
                continue;
            }
            fresh.Add(item);
        }

        if (fresh.Count == 0) return;

        var keys = new List<string>(fresh.Count);
        foreach (var f in fresh) keys.Add(f.Instruction.IdempotencyKey);
        var reserved = await _store.ReserveIdempotencyKeysAsync(keys, CancellationToken.None)
            .ConfigureAwait(false);
        var reservedSet = new HashSet<string>(reserved, StringComparer.Ordinal);

        var accepted = new List<Item>(fresh.Count);
        foreach (var item in fresh)
        {
            if (reservedSet.Contains(item.Instruction.IdempotencyKey))
            {
                accepted.Add(item);
            }
            else
            {
                Interlocked.Increment(ref _deduped);
                item.Tcs.TrySetResult(ProcessResult.Ok(default, item.CorrelationId)); // replay-safe ack
            }
        }
        if (accepted.Count == 0) return;

        // ---- 2. Deterministic ordering (stable across workers/retries) ----
        accepted.Sort(static (a, b) => string.CompareOrdinal(a.Instruction.IdempotencyKey, b.Instruction.IdempotencyKey));

        // ---- 3. Load all touched accounts in one round trip ----
        var idList = new List<AccountId>(accepted.Count * 2);
        var idSet = new HashSet<AccountId>();
        foreach (var item in accepted)
        {
            if (item.Instruction.FromAccount is { } from && idSet.Add(from)) idList.Add(from);
            if (idSet.Add(item.Instruction.ToAccount)) idList.Add(item.Instruction.ToAccount);
        }

        var loaded = await _store.LoadAccountsAsync(idList, CancellationToken.None).ConfigureAwait(false);
        var accounts = new Dictionary<AccountId, Account>(loaded.Count);
        foreach (var a in loaded) accounts[a.Id] = a;

        // ---- 4. Apply domain rules in memory ----
        var entries = new List<LedgerEntry>(accepted.Count * 2);
        var dirty = new List<Account>(accounts.Count);
        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (var item in accepted)
        {
            var ins = item.Instruction;
            Money amount = new(ins.AmountMinorUnits, ins.CurrencyCode);
            try
            {
                if (ins.IsTransfer)
                {
                    Account src = Require(accounts, ins.FromAccount!.Value);
                    Account dst = Require(accounts, ins.ToAccount);

                    var debitLeg = src.PostDebit(amount, item.CorrelationId, ins.ToAccount, ins.Reference, now);
                    var creditLeg = dst.PostCredit(amount, item.CorrelationId, ins.Reference, now);
                    // Re-tag the credit leg with counterparty info for the transfer pair:
                    var creditEntry = new LedgerEntry
                    {
                        Id = creditLeg.Id,
                        AccountId = dst.Id,
                        Amount = amount,
                        Type = TransactionType.Credit,
                        Status = TransactionStatus.Posted,
                        CorrelationId = item.CorrelationId,
                        CounterpartyAccountId = src.Id,
                        OccurredAtUtc = now,
                        Reference = ins.Reference,
                        Sequence = dst.LastSequence
                    };

                    entries.Add(debitLeg);
                    entries.Add(creditEntry);
                    item.Result = ProcessResult.Ok(debitLeg.Id, item.CorrelationId);
                    item.Applied = true;
                }
                else
                {
                    Account dst = Require(accounts, ins.ToAccount);
                    var e = dst.PostCredit(amount, item.CorrelationId, ins.Reference, now);
                    entries.Add(e);
                    item.Result = ProcessResult.Ok(e.Id, item.CorrelationId);
                    item.Applied = true;
                }
            }
            catch (DomainException ex)
            {
                Interlocked.Increment(ref _rejected);
                item.Result = ProcessResult.Fail(ex.Message);
                item.Applied = false;
            }
        }

        // Build the truly-dirty list: any account that produced entries in this pass.
        dirty.Clear();
        var dirtySet = new HashSet<AccountId>();
        foreach (var e in entries)
            if (dirtySet.Add(e.AccountId)) dirty.Add(accounts[e.AccountId]);

        // Fail immediately those rejected before commit.
        foreach (var item in accepted.Where(i => !i.Applied))
            item.Tcs.TrySetResult(item.Result);

        if (dirty.Count == 0) return;

        // ---- 5. One durable batched commit, with optimistic-concurrency retries ----
        bool committed = false;
        for (int attempt = 0; attempt <= _options.MaxRetries && !committed; attempt++)
        {
            try
            {
                await _store.SaveBatchAsync(dirty, entries, CancellationToken.None).ConfigureAwait(false);
                committed = true;
            }
            catch (ConcurrencyConflictException) when (attempt < _options.MaxRetries)
            {
                _logger.LogWarning("Version conflict on batch of {Count}; retry {Attempt}",
                    dirty.Count, attempt + 1);
                entries.Clear();
                await ReapplyAsync(dirty, entries, accepted).ConfigureAwait(false);
            }
        }

        if (!committed)
        {
            foreach (var item in accepted.Where(i => i.Applied))
                item.Tcs.TrySetResult(ProcessResult.Fail("Rejected after concurrent update; retry later."));
            return;
        }

        Interlocked.Add(ref _processed, entries.Count);
        foreach (var item in accepted.Where(i => i.Applied))
            item.Tcs.TrySetResult(item.Result);
        foreach (var e in entries)
            _events.Publish(e);
    }

    private static Account Require(Dictionary<AccountId, Account> accounts, AccountId id) =>
        accounts.TryGetValue(id, out var a) ? a : throw new AccountNotFoundException(id);

    /// <summary>After a version conflict: reload latest state and re-apply every applied instruction.</summary>
    private async Task ReapplyAsync(List<Account> dirty, List<LedgerEntry> entries, List<Item> accepted)
    {
        var ids = new List<AccountId>(dirty.Count);
        foreach (var a in dirty) ids.Add(a.Id);
        var latest = await _store.LoadAccountsAsync(ids, CancellationToken.None).ConfigureAwait(false);
        var map = new Dictionary<AccountId, Account>(latest.Count);
        foreach (var a in latest) map[a.Id] = a;

        dirty.Clear();
        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (var item in accepted)
        {
            if (!item.Applied) continue;
            var ins = item.Instruction;
            Money amount = new(ins.AmountMinorUnits, ins.CurrencyCode);
            try
            {
                if (ins.IsTransfer)
                {
                    Account src = Require(map, ins.FromAccount!.Value);
                    Account dst = Require(map, ins.ToAccount);
                    entries.Add(src.PostDebit(amount, item.CorrelationId, ins.ToAccount, ins.Reference, now));
                    var cl = dst.PostCredit(amount, item.CorrelationId, ins.Reference, now);
                    entries.Add(new LedgerEntry
                    {
                        Id = cl.Id, AccountId = dst.Id, Amount = amount,
                        Type = TransactionType.Credit, Status = TransactionStatus.Posted,
                        CorrelationId = item.CorrelationId, CounterpartyAccountId = src.Id,
                        OccurredAtUtc = now, Reference = ins.Reference, Sequence = dst.LastSequence
                    });
                }
                else
                {
                    Account dst = Require(map, ins.ToAccount);
                    entries.Add(dst.PostCredit(amount, item.CorrelationId, ins.Reference, now));
                }
            }
            catch (DomainException ex)
            {
                item.Applied = false;
                item.Result = ProcessResult.Fail(ex.Message);
                Interlocked.Increment(ref _rejected);
            }
        }

        var seenAcc = new HashSet<AccountId>();
        foreach (var e in entries)
            if (seenAcc.Add(e.AccountId)) dirty.Add(map[e.AccountId]);

        foreach (var item in accepted.Where(i => !i.Applied))
            item.Tcs.TrySetResult(item.Result);
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        try { await _aggregatorLoop.ConfigureAwait(false); }
        catch (Exception ex) { _logger.LogWarning(ex, "Aggregator loop terminated on shutdown."); }
    }

    // Exposed for Prometheus gauges.
    public long StatsProcessed => Interlocked.Read(ref _processed);
    public long StatsRejected => Interlocked.Read(ref _rejected);
    public long StatsDeduped => Interlocked.Read(ref _deduped);
    public long StatsQueued => Interlocked.Read(ref _queued);
}
