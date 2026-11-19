namespace Banking.Domain;

public enum AccountStatus : byte
{
    Active = 0,
    Frozen = 1,   // compliance hold — no debits
    Closed = 2
}

/// <summary>
/// Account aggregate. Balance is maintained incrementally (denormalized) so reads
/// never require a full ledger scan. Concurrency is protected by an optimistic
/// version token — retries are cheap and lock-free at the DB level.
/// </summary>
public sealed class Account
{
    // Overdraft allowance in minor units (>= 0). Default accounts cannot go negative.
    public const long StandardOverdraftMinorUnits = 0;

    public AccountId Id { get; init; }
    public Guid CustomerId { get; init; }
    public string Iban { get; init; } = string.Empty;
    public ushort CurrencyCode { get; init; }              // ISO-4217 numeric
    public Money Balance { get; private set; }
    public long OverdraftLimitMinorUnits { get; init; } = StandardOverdraftMinorUnits;
    public AccountStatus Status { get; private set; }
    public long Version { get; private set; }               // optimistic concurrency token
    public long LastSequence { get; private set; }          // per-account entry sequence
    public DateTimeOffset CreatedAtUtc { get; init; }
    public DateTimeOffset UpdatedAtUtc { get; private set; }

    private Money Minor(long value) => new(value, CurrencyCode);

    public bool CanDebit(Money amount) =>
        Status == AccountStatus.Active &&
        (Balance.Subtract(amount).MinorUnits >= -OverdraftLimitMinorUnits);

    /// <summary>Posts a credit leg. Returns the resulting ledger entry data.</summary>
    public LedgerEntry PostCredit(Money amount, Guid correlationId, string? reference, DateTimeOffset now)
    {
        EnsureActive();
        if (!amount.IsPositive)
            throw new DomainException("Credit amount must be positive.");

        Balance = Balance.Add(amount);
        Bump(now);

        return Entry(TransactionId.NewV7(), amount, TransactionType.Credit, correlationId, null, reference, now);
    }

    /// <summary>Posts a debit leg, enforcing overdraft rules.</summary>
    public LedgerEntry PostDebit(Money amount, Guid correlationId, AccountId? counterparty, string? reference, DateTimeOffset now)
    {
        EnsureActive();
        if (!amount.IsPositive)
            throw new DomainException("Debit amount must be positive.");
        if (!CanDebit(amount))
            throw new InsufficientFundsException(Id, amount, Balance);

        Balance = Balance.Subtract(amount);
        Bump(now);

        return Entry(TransactionId.NewV7(), amount.Negate(), TransactionType.Debit, correlationId, counterparty, reference, now);
    }

    /// <summary>Applies a compensating reversal for a previously posted entry.</summary>
    public LedgerEntry PostReversal(LedgerEntry original, Guid correlationId, DateTimeOffset now)
    {
        EnsureActive();
        Money flipped = new(-original.Amount.MinorUnits, original.Amount.CurrencyCode);
        Balance = Balance.Add(flipped);
        Bump(now);

        return Entry(TransactionId.NewV7(), flipped, TransactionType.Reversal, correlationId,
                     original.AccountId, $"reversal:{original.Id}", now);
    }

    private LedgerEntry Entry(TransactionId id, Money signedAmount, TransactionType type,
        Guid correlationId, AccountId? counterparty, string? reference, DateTimeOffset now) => new()
    {
        Id = id,
        AccountId = Id,
        Amount = signedAmount,
        Type = type,
        Status = TransactionStatus.Posted,
        CorrelationId = correlationId,
        CounterpartyAccountId = counterparty,
        OccurredAtUtc = now,
        Reference = reference,
        Sequence = ++LastSequence
    };

    private void EnsureActive()
    {
        if (Status != AccountStatus.Active)
            throw new DomainException($"Account {Id} is {Status}; operation not allowed.");
    }

    private void Bump(DateTimeOffset now)
    {
        Version++;
        UpdatedAtUtc = now;
    }

    // ---- Factory / rehydration ----

    public static Account Create(Guid customerId, string iban, ushort currencyCode, Money openingBalance, DateTimeOffset now)
    {
        if (openingBalance.MinorUnits < 0)
            throw new DomainException("Opening balance cannot be negative.");

        return new Account
        {
            Id = AccountId.NewV7(),
            CustomerId = customerId,
            Iban = iban,
            CurrencyCode = currencyCode,
            Balance = openingBalance,
            Status = AccountStatus.Active,
            CreatedAtUtc = now,
            UpdatedAtUtc = now
        };
    }

    /// <summary>Used by persistence infrastructure to rehydrate the aggregate with its version token.</summary>
    public static Account Rehydrate(
        AccountId id, Guid customerId, string iban, ushort currencyCode,
        long balanceMinorUnits, long overdraftMinorUnits, AccountStatus status,
        long version, long lastSequence, DateTimeOffset createdAt, DateTimeOffset updatedAt)
    {
        var acc = new Account
        {
            Id = id,
            CustomerId = customerId,
            Iban = iban,
            CurrencyCode = currencyCode,
            OverdraftLimitMinorUnits = overdraftMinorUnits,
            CreatedAtUtc = createdAt
        };
        acc.Balance = new Money(balanceMinorUnits, currencyCode);
        acc.Status = status;
        acc.Version = version;
        acc.LastSequence = lastSequence;
        acc.UpdatedAtUtc = updatedAt;
        return acc;
    }
}
