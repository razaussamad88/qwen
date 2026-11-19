namespace Banking.Shared.Messaging;

/// <summary>
/// Single source of truth for Kafka topic names and message contracts shared by
/// BFF, Gateway and all microservices. Topics are partitioned by account id so
/// per-account ordering is guaranteed by Kafka itself.
/// </summary>
public static class KafkaTopics
{
    /// <summary>Command topic: BFF -> Transactions service (queue-like consumer group).</summary>
    public const string TransactionCommands = "banking.transaction.commands.v1";

    /// <summary>Event topic: Transactions service -> anyone (accounts projection, notifications, fraud...).</summary>
    public const string TransactionEvents = "banking.transaction.events.v1";

    /// <summary>Event topic: Accounts service -> projections (account opened/closed/frozen).</summary>
    public const string AccountEvents = "banking.account.events.v1";

    public const string ConsumerGroupTransactions = "transactions-service";
    public const string ConsumerGroupAccountsProjection = "accounts-projection";
    public const string ConsumerGroupBff = "bff";
}

/// <summary>Common envelope: correlation/causation ids, tenant, timestamps.</summary>
public sealed record MessageEnvelope
{
    public required Guid MessageId { get; init; }
    public required Guid CorrelationId { get; init; }
    public Guid? CausationId { get; init; }
    public required DateTimeOffset ProducedAtUtc { get; init; }
    public string Producer { get; init; } = string.Empty;
    public string TenantId { get; init; } = "default";
}

/// <summary>
/// Command emitted by the BFF onto <see cref="KafkaTopics.TransactionCommands"/>.
/// Keyed by the debit account id (or credit account for deposits) so all
/// instructions touching one account land on the same partition => strict ordering.
/// </summary>
public sealed record TransactionCommandMessage
{
    public required MessageEnvelope Envelope { get; init; }
    public required string IdempotencyKey { get; init; }
    public required string CommandType { get; init; }   // "transfer" | "deposit" | "withdrawal"
    public Guid? FromAccountId { get; init; }
    public required Guid ToAccountId { get; init; }
    /// <summary>Amount in minor units (cents). Always positive.</summary>
    public required long AmountMinorUnits { get; init; }
    public required ushort CurrencyCode { get; init; }
    public string? Reference { get; init; }
}

/// <summary>
/// Domain event emitted by the Transactions service onto
/// <see cref="KafkaTopics.TransactionEvents"/> after durable commit (outbox relay).
/// One message per transfer carries BOTH legs so consumers never see half a transaction.
/// </summary>
public sealed record TransactionPostedEvent
{
    public required MessageEnvelope Envelope { get; init; }
    public required string IdempotencyKey { get; init; }
    public required string TransactionType { get; init; }  // "transfer" | "deposit"
    public Guid? FromAccountId { get; init; }
    public required Guid ToAccountId { get; init; }
    public required long AmountMinorUnits { get; init; }
    public required ushort CurrencyCode { get; init; }
    public required long FromBalanceAfterMinorUnits { get; init; }
    public required long ToBalanceAfterMinorUnits { get; init; }
    public required IReadOnlyList<string> TransactionIds { get; init; }
    public string? Reference { get; init; }
    public required DateTimeOffset PostedAtUtc { get; init; }
}

/// <summary>Ledger-state snapshot broadcast to the read side / caches.</summary>
public sealed record AccountBalanceUpdatedEvent
{
    public required MessageEnvelope Envelope { get; init; }
    public required Guid AccountId { get; init; }
    public required long BalanceMinorUnits { get; init; }
    public required long Version { get; init; }
    public required DateTimeOffset UpdatedAtUtc { get; init; }
}
