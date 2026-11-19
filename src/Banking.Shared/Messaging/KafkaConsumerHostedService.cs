using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Banking.Shared.Messaging;

/// <summary>Handler contract implemented by each microservice's consumer logic.</summary>
public interface IKafkaMessageHandler<TValue>
{
    /// <summary>Return false to treat the message as poison and route it to the DLQ.</summary>
    Task<bool> HandleAsync(string key, TValue value, MessageMetadata meta, CancellationToken ct);
}

public readonly record struct MessageMetadata(
    string Topic, int Partition, long Offset, DateTimeOffset TimestampUtc,
    string? CorrelationId, int Attempt);

/// <summary>
/// Resilient generic Kafka consumer hosted service:
///  - single IConsumer loop, batches drained up to MaxBatchMessages / BatchLingerMs
///  - manual offset store + commit AFTER successful batch processing => at-least-once
///  - transient handler failures retried in-process (bounded), then DLQ — never stalls a partition
///  - poison/undecodable records go straight to "&lt;topic&gt;.dlq" with diagnostics headers
///  - graceful drain on shutdown (commit what we have, close consumer)
/// Scaling model: run N replicas of the service pod; Kafka rebalances partitions
/// across consumers automatically (Kubernetes HPA can scale on lag metrics).
/// </summary>
public sealed class KafkaConsumerHostedService<TValue> : BackgroundService
{
    private readonly KafkaOptions _kafka;
    private readonly ConsumerGroupOptions _group;
    private readonly IKafkaMessageHandler<TValue> _handler;
    private readonly IKafkaProducerFactory _producers;
    private readonly ILogger _logger;
    private long _consumed, _failed;

    public KafkaConsumerHostedService(
        IOptions<KafkaOptions> kafka,
        IOptions<ConsumerGroupOptions> group,
        IKafkaMessageHandler<TValue> handler,
        IKafkaProducerFactory producers,
        ILogger<KafkaConsumerHostedService<TValue>> logger)
    {
        _kafka = kafka.Value;
        _group = group.Value;
        _handler = handler;
        _producers = producers;
        _logger = logger;
    }

    public override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var dlq = _producers.CreateRaw();
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _group.GroupId,
            ClientId = _group.ClientId,
            EnableAutoOffsetStore = false, // manual => process-then-commit
            AutoOffsetReset = string.Equals(_group.AutoOffsetReset, "earliest", StringComparison.OrdinalIgnoreCase)
                ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
            MaxPollIntervalMs = _kafka.MaxPollIntervalMs,
            ReceiveMessageMaxBytes = _kafka.MessageMaxBytes,
            SocketReceiveBufferBytes = _kafka.ReceiveBufferBytes,
            SecurityProtocol = Enum.TryParse<SecurityProtocol>(_kafka.SecurityProtocol, true, out var sp)
                ? sp : Confluent.Kafka.SecurityProtocol.Plaintext,
            SaslMechanism = Enum.TryParse<SaslMechanism>(_kafka.SaslMechanism, true, out var sm) ? sm : null,
            SaslUsername = _kafka.SaslUsername,
            SaslPassword = _kafka.SaslPassword,
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogWarning("Kafka error (group {Group}): {Error}", _group.GroupId, e.Reason))
            .SetPartitionsAssignedHandler((_, ps) =>
                _logger.LogInformation("Assigned {Count} partitions: {Partitions}", ps.Count,
                    string.Join(", ", ps.Select(p => $"{p.Topic}-{p.Partition}"))))
            .Build();

        consumer.Subscribe(_group.Topics);
        var batch = new List<IConsumeResult<string, byte[]>>(_group.MaxBatchMessages);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                batch.Clear();
                var deadline = DateTime.UtcNow.AddMilliseconds(_group.BatchLingerMs);

                // Drain quickly: poll until batch full OR linger elapsed.
                while (batch.Count < _group.MaxBatchMessages)
                {
                    var remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero && batch.Count > 0) break;
                    var cr = consumer.Consume(remaining > TimeSpan.Zero
                        ? (int)remaining.TotalMilliseconds : 1);
                    if (cr is null || cr.IsPartitionEOF) continue;
                    batch.Add(cr);
                }

                if (batch.Count == 0)
                {
                    // Nothing arrived within linger window; tiny sleep avoids hot-looping.
                    await Task.Delay(2, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await ProcessBatchAsync(batch, dlq, consumer, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        finally
        {
            try { consumer.Close(); } catch (Exception ex) { _logger.LogWarning(ex, "Consumer close failed."); }
            _logger.LogInformation("Consumer stopped. consumed={Consumed} dlq={Failed}", _consumed, _failed);
        }
    }

    private async Task ProcessBatchAsync(
        List<IConsumeResult<string, byte[]>> batch,
        IProducer<string, byte[]> dlq,
        IConsumer<string, byte[]> consumer,
        CancellationToken ct)
    {
        foreach (var cr in batch)
        {
            Interlocked.Increment(ref _consumed);
            var meta = new MessageMetadata(
                cr.Topic, cr.Partition.Value, cr.Offset.Value,
                cr.Message.Timestamp.UnixTimestampMs > 0
                    ? DateTimeOffset.FromUnixTimeMilliseconds(cr.Message.Timestamp.UnixTimestampMs)
                    : DateTimeOffset.UtcNow,
                GetHeader(cr, "correlation-id"), GetHeader(cr, "x-retry"));

            TValue? value = default;
            bool decodable = true;
            if (cr.Message.Value is { Length: > 0 })
            {
                try { value = JsonSerializer.Deserialize<TValue>(cr.Message.Value, KafkaJson.Options); }
                catch (JsonException) { decodable = false; }
            }
            else decodable = false;

            if (!decodable || value is null)
            {
                await SendToDlqAsync(dlq, cr, "undecodable-payload", 0, ct).ConfigureAwait(false);
                continue;
            }

            bool ok = false;
            for (int attempt = 1; attempt <= _group.MaxRetriesPerMessage + 1 && !ok; attempt++)
            {
                try
                {
                    ok = await _handler.HandleAsync(cr.Message.Key ?? "", value, meta with { Attempt = attempt }, ct)
                        .ConfigureAwait(false);
                    if (!ok) break; // business rejection -> DLQ directly, retrying won't help
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex) when (attempt <= _group.MaxRetriesPerMessage)
                {
                    _logger.LogWarning(ex, "Handler failed (attempt {Attempt}) at {Topic}-{Partition}@{Offset}; backing off.",
                        attempt, cr.Topic, cr.Partition.Value, cr.Offset.Value);
                    await Task.Delay(TimeSpan.FromMilliseconds(50 * attempt), ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Handler exhausted retries at {Topic}-{Partition}@{Offset}; sending to DLQ.",
                        cr.Topic, cr.Partition.Value, cr.Offset.Value);
                    ok = false;
                }
            }

            if (!ok)
                await SendToDlqAsync(dlq, cr, "handler-rejected-or-exhausted", _group.MaxRetriesPerMessage, ct)
                    .ConfigureAwait(false);
        }

        // Commit offsets after the whole batch has been dealt with (at-least-once semantics).
        consumer.Commit(batch[^1]);
    }

    private async Task SendToDlqAsync(
        IProducer<string, byte[]> dlq, IConsumeResult<string, byte[]> cr,
        string reason, int attempts, CancellationToken ct)
    {
        Interlocked.Increment(ref _failed);
        var msg = new Message<string, byte[]>
        {
            Key = cr.Message.Key,
            Value = cr.Message.Value,
            Headers = new Headers()
        };
        msg.Headers.TryAddLast("dlq-reason", Encoding.UTF8.GetBytes(reason));
        msg.Headers.TryAddLast("dlq-source-topic", Encoding.UTF8.GetBytes(cr.Topic));
        msg.Headers.TryAddLast("dlq-source-partition", Encoding.UTF8.GetBytes(cr.Partition.Value.ToString()));
        msg.Headers.TryAddLast("dlq-source-offset", Encoding.UTF8.GetBytes(cr.Offset.Value.ToString()));
        msg.Headers.TryAddLast("dlq-attempts", Encoding.UTF8.GetBytes(attempts.ToString()));

        var topic = cr.Topic + ".dlq";
        var result = await dlq.ProduceAsync(topic, msg, ct).ConfigureAwait(false);
        _logger.LogWarning("Routed poison message to {Topic} @ {Partition}:{Offset} ({Reason})",
            result.Topic, result.Partition.Value, result.Offset.Value, reason);
    }

    private static string? GetHeader(IConsumeResult<string, byte[]> cr, string name)
    {
        if (cr.Message.Headers is null) return null;
        var h = cr.Message.Headers.TryGetFirst(name);
        return h is null ? null : Encoding.UTF8.GetString(h.GetValueBytes());
    }
}
