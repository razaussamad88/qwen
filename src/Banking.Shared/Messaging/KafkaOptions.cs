namespace Banking.Shared.Messaging;

/// <summary>Kafka connection/producer/consumer settings, bound from config section "Kafka".</summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    /// <summary>Comma-separated bootstrap servers, e.g. "kafka-0.kafka:9092,kafka-1.kafka:9092".</summary>
    public string BootstrapServers { get; set; } = "localhost:9092";

    public int MessageMaxBytes { get; set; } = 4_000_000;

    // Producer tuning for high TPS
    public int LingerMs { get; set; } = 5;              // batch small windows -> huge throughput gains
    public int BatchSize { get; set; } = 262_144;       // 256 KiB
    public string CompressionType { get; set; } = "lz4";
    public int MaxInFlightRequests { get; set; } = 5;
    public bool AcksLeaderOnly { get; set; } = true;    // "1": leader ack; use false => "all" for strict durability

    // Consumer tuning
    public int MaxPollIntervalMs { get; set; } = 300_000;
    public int ReceiveBufferBytes { get; set; } = 32_000_000;

    public string SecurityProtocol { get; set; } = "plaintext"; // plaintext|ssl|sasl_ssl
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string? SchemaRegistryUrl { get; set; }      // reserved for Avro/Protobuf evolution
}
