namespace Banking.Shared.Messaging;

/// <summary>Per-service consumer settings, bound from config section "KafkaConsumer".</summary>
public sealed class ConsumerGroupOptions
{
    public const string SectionName = "KafkaConsumer";

    public required string GroupId { get; set; }
    public required string[] Topics { get; set; }
    public string ClientId { get; set; } = Environment.MachineName;

    /// <summary>Max messages drained per commit cycle (bigger = throughput, smaller = latency).</summary>
    public int MaxBatchMessages { get; set; } = 500;

    /// <summary>How long to wait to fill a consume batch.</summary>
    public int BatchLingerMs { get; set; } = 20;

    /// <summary>In-process retries before routing to the DLQ.</summary>
    public int MaxRetriesPerMessage { get; set; } = 3;

    /// <summary>"latest" (default) or "earliest" (replay from beginning on first start).</summary>
    public string AutoOffsetReset { get; set; } = "latest";
}
