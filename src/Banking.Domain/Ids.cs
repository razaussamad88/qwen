namespace Banking.Domain;

/// <summary>
/// Strongly-typed id wrappers. Version-7 UUIDs are used throughout: they are
/// time-ordered, which keeps B-tree index inserts append-only (no page splits)
/// — critical for write throughput at 50k+ TPS.
/// </summary>
public readonly record struct AccountId(Guid Value)
{
    public static AccountId NewV7() => new(UuidV7.New());
    public override string ToString() => Value.ToString();
    public static implicit operator Guid(AccountId id) => id.Value;
}

public readonly record struct TransactionId(Guid Value)
{
    public static TransactionId NewV7() => new(UuidV7.New());
    public override string ToString() => Value.ToString();
    public static implicit operator Guid(TransactionId id) => id.Value;
}

/// <summary>Client-supplied idempotency key (dedupe token).</summary>
public readonly record struct IdempotencyKey(string Value)
{
    public override string ToString() => Value;
}
