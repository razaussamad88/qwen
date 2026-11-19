using Banking.Domain;

namespace Banking.Application;

/// <summary>Result of submitting a single transaction instruction.</summary>
public readonly record struct ProcessResult(
    bool Accepted,
    TransactionId TransactionId,
    Guid CorrelationId,
    string? Error)
{
    public static ProcessResult Ok(TransactionId id, Guid correlationId) => new(true, id, correlationId, null);
    public static ProcessResult Fail(string error) => new(false, default, default, error);
}

/// <summary>One atomic processing unit. A transfer is a single unit with two legs.</summary>
public sealed class TransactionInstruction
{
    /// <summary>Client idempotency key. Duplicate keys are deduplicated for 24h.</summary>
    public required string IdempotencyKey { get; init; }
    public AccountId? FromAccount { get; init; }
    public required AccountId ToAccount { get; init; }
    /// <summary>Amount in minor units (cents). Always positive.</summary>
    public required long AmountMinorUnits { get; init; }
    public required ushort CurrencyCode { get; init; }
    public string? Reference { get; init; }

    /// <summary>True when money moves between two distinct accounts (two ledger legs).</summary>
    public bool IsTransfer => FromAccount is { } from && from != ToAccount;
}

/// <summary>Persistence abstraction implemented by Infrastructure (Postgres/Redis/etc.).</summary>
public interface ITransactionStore
{
    Task<Account?> LoadAccountAsync(AccountId id, CancellationToken ct);
    Task<IReadOnlyList<Account>> LoadAccountsAsync(IReadOnlyList<AccountId> ids, CancellationToken ct);

    /// <summary>
    /// Durably commits updated accounts + appended ledger entries in one atomic write.
    /// Throws <see cref="ConcurrencyConflictException"/> if any account version token is stale.
    /// </summary>
    Task SaveBatchAsync(IReadOnlyList<Account> accounts, IReadOnlyList<LedgerEntry> entries, CancellationToken ct);

    /// <summary>Atomically reserves idempotency keys. Returns the subset that was newly reserved.</summary>
    Task<IReadOnlyList<string>> ReserveIdempotencyKeysAsync(IReadOnlyList<string> keys, CancellationToken ct);
}

/// <summary>Fire-and-forget sink for domain events (outbox → Kafka/NATS in production).</summary>
public interface IEventSink
{
    void Publish(LedgerEntry entry);
}

public interface ITransactionProcessor
{
    /// <summary>Enqueue one instruction; completes when its batch has been durably committed.</summary>
    ValueTask<ProcessResult> SubmitAsync(TransactionInstruction instruction, CancellationToken ct = default);

    /// <summary>Submit many instructions concurrently; results come back in input order.</summary>
    IAsyncEnumerable<ProcessResult> SubmitBatchAsync(IReadOnlyList<TransactionInstruction> instructions, CancellationToken ct = default);
}

public interface IAccountQueryService
{
    ValueTask<AccountSnapshot?> GetAccountAsync(AccountId id, CancellationToken ct = default);
}

public readonly record struct AccountSnapshot(
    AccountId Id, string Iban, long BalanceMinorUnits, ushort CurrencyCode,
    byte Status, long Version, DateTimeOffset UpdatedAtUtc)
{
    public static AccountSnapshot From(Account a) =>
        new(a.Id, a.Iban, a.Balance.MinorUnits, a.CurrencyCode, (byte)a.Status, a.Version, a.UpdatedAtUtc);
}

/// <summary>Thrown by stores when an optimistic version check fails.</summary>
public class ConcurrencyConflictException : Exception
{
    public ConcurrencyConflictException(string message) : base(message) { }
}
