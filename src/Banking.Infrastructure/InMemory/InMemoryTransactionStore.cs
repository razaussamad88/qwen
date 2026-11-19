using System.Collections.Concurrent;
using Banking.Application;
using Banking.Domain;

namespace Banking.Infrastructure.InMemory;

/// <summary>
/// Fully in-memory store used for local runs, unit/integration tests and as the
/// reference implementation of <see cref="ITransactionStore"/> semantics
/// (optimistic version conflicts, atomic idempotency reservation).
/// Swap for Postgres/Redis via DI configuration in production.
/// </summary>
public sealed class InMemoryTransactionStore : ITransactionStore
{
    private readonly ConcurrentDictionary<AccountId, Account> _accounts = new();
    private readonly ConcurrentDictionary<string, byte> _idempotencyKeys = new(StringComparer.Ordinal);
    private readonly ConcurrentQueue<LedgerEntry> _ledger = new();

    public Account CreateAccount(Guid customerId, string iban, ushort currencyCode, long openingMinorUnits)
    {
        var acc = Account.Create(customerId, iban, currencyCode,
            new Money(openingMinorUnits, currencyCode), DateTimeOffset.UtcNow);
        _accounts[acc.Id] = acc;
        return acc;
    }

    public Task<Account?> LoadAccountAsync(AccountId id, CancellationToken ct) =>
        Task.FromResult(_accounts.TryGetValue(id, out var a) ? a : null);

    public Task<IReadOnlyList<Account>> LoadAccountsAsync(IReadOnlyList<AccountId> ids, CancellationToken ct)
    {
        var list = new List<Account>(ids.Count);
        foreach (var id in ids)
            if (_accounts.TryGetValue(id, out var a)) list.Add(a);
        return Task.FromResult<IReadOnlyList<Account>>(list);
    }

    public Task SaveBatchAsync(IReadOnlyList<Account> accounts, IReadOnlyList<LedgerEntry> entries, CancellationToken ct)
    {
        // Count how many version bumps each account received inside this batch.
        var bumps = new Dictionary<AccountId, long>();
        foreach (var e in entries)
            bumps[e.AccountId] = bumps.TryGetValue(e.AccountId, out var n) ? n + 1 : 1;

        // Emulate CAS on the version token, like `UPDATE ... WHERE version = @expected`.
        foreach (var acc in accounts)
        {
            if (!_accounts.TryGetValue(acc.Id, out var current))
                throw new AccountNotFoundException(acc.Id);

            long expectedVersion = acc.Version - (bumps.TryGetValue(acc.Id, out var b) ? b : 0);
            if (current.Version != expectedVersion)
                throw new ConcurrencyConflictException($"Account {acc.Id} version drift.");

            if (!_accounts.TryUpdate(acc.Id, acc, current))
                throw new ConcurrencyConflictException($"Account {acc.Id} concurrent update.");
        }

        foreach (var e in entries) _ledger.Enqueue(e);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ReserveIdempotencyKeysAsync(IReadOnlyList<string> keys, CancellationToken ct)
    {
        var newly = new List<string>(keys.Count);
        foreach (var k in keys)
            if (_idempotencyKeys.TryAdd(k, 1)) newly.Add(k);
        return Task.FromResult<IReadOnlyList<string>>(newly);
    }

    // ---- Test/inspection helpers ----
    public IReadOnlyCollection<LedgerEntry> LedgerSnapshot() => _ledger.ToArray();
    public int AccountCount => _accounts.Count;
    public int IdempotencyKeyCount => _idempotencyKeys.Count;
}

public sealed class NullEventSink : IEventSink
{
    public void Publish(LedgerEntry entry) { }
}

/// <summary>Circulating in-memory event sink with bounded buffer (drops oldest under pressure).</summary>
public sealed class RingBufferEventSink : IEventSink
{
    private readonly ConcurrentQueue<LedgerEntry> _ring = new();
    private readonly int _capacity;

    public RingBufferEventSink(int capacity = 100_000) => _capacity = capacity;

    public void Publish(LedgerEntry entry)
    {
        _ring.Enqueue(entry);
        while (_ring.Count > _capacity) _ring.TryDequeue(out _);
    }

    public IReadOnlyCollection<LedgerEntry> Snapshot() => _ring.ToArray();
}
