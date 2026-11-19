using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Banking.Shared.Messaging;

/// <summary>
/// Creates tuned Kafka producers. One IProducer instance is thread-safe and shared
/// process-wide (Confluent.Kafka batches internally) — do NOT create per request.
/// </summary>
public interface IKafkaProducerFactory
{
    IProducer<string, TValue> CreateValueSerializer<TValue>();
    IProducer<string, byte[]> CreateRaw();
}

public sealed class KafkaProducerFactory : IKafkaProducerFactory, IDisposable
{
    private readonly KafkaOptions _options;
    private readonly List<IDisposable> _created = new();
    private readonly object _gate = new();

    public KafkaProducerFactory(IOptions<KafkaOptions> options) => _options = options.Value;

    public IProducer<string, TValue> CreateValueSerializer<TValue>()
    {
        var p = new ProducerBuilder<string, TValue>(BuildConfig())
            .SetValueSerializer(new SystemTextJsonSerializer<TValue>())
            .Build();
        lock (_gate) _created.Add(p);
        return p;
    }

    public IProducer<string, byte[]> CreateRaw()
    {
        var p = new ProducerBuilder<string, byte[]>(BuildConfig())
            .SetValueSerializer(Sys.Serialization.ByteArraySerializer.Default)
            .Build();
        lock (_gate) _created.Add(p);
        return p;
    }

    private ProducerConfig BuildConfig() => new()
    {
        BootstrapServers = _options.BootstrapServers,
        Acks = _options.AcksLeaderOnly ? Acks.Leader : Acks.All,
        LingerMs = _options.LingerMs,
        BatchSize = _options.BatchSize,
        CompressionType = Enum.TryParse<CompressionType>(_options.CompressionType, ignoreCase: true, out var c)
            ? c : CompressionType.Lz4,
        MaxInFlight = _options.MaxInFlightRequests,
        MessageMaxBytes = _options.MessageMaxBytes,
        SecurityProtocol = Enum.TryParse<SecurityProtocol>(_options.SecurityProtocol, ignoreCase: true, out var sp)
            ? sp : Confluent.Kafka.SecurityProtocol.Plaintext,
        SaslMechanism = Enum.TryParse<SaslMechanism>(_options.SaslMechanism, ignoreCase: true, out var sm)
            ? sm : null,
        SaslUsername = _options.SaslUsername,
        SaslPassword = _options.SaslPassword,
    };

    public void Dispose()
    {
        lock (_gate) foreach (var d in _created) d.Dispose();
    }
}
