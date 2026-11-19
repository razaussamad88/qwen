namespace Banking.Domain;

public enum TransactionType : byte
{
    Credit = 1,          // deposit / incoming transfer leg
    Debit = 2,           // withdrawal / outgoing transfer leg
    Transfer = 3,        // compound transfer (two legs)
    Reversal = 4         // compensating entry
}

public enum TransactionStatus : byte
{
    Pending = 0,
    Posted = 1,
    Rejected = 2,
    Reversed = 3
}

/// <summary>
/// A single immutable ledger journal entry (one leg of a transaction).
/// The ledger is append-only: entries are never updated or deleted.
/// Balance is derived (or maintained incrementally on the account aggregate).
/// </summary>
public sealed class LedgerEntry
{
    public required TransactionId Id { get; init; }
    public required AccountId AccountId { get; init; }
    public required Money Amount { get; init; }             // signed: negative = debit from account
    public required TransactionType Type { get; init; }
    public required TransactionStatus Status { get; init; }
    public required Guid CorrelationId { get; init; }       // groups both legs of a transfer
    public AccountId? CounterpartyAccountId { get; init; }
    public required DateTimeOffset OccurredAtUtc { get; init; }
    public string? Reference { get; init; }
    public long Sequence { get; init; }                     // per-account monotonically increasing
}
