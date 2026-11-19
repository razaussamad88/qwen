using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

namespace Banking.Shared.Messaging;

/// <summary>Shared, source-gen-friendly JSON options for all Kafka payloads.</summary>
public static class KafkaJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };
}

/// <summary>System.Text.Json serializer for Confluent.Kafka (schema-less JSON payloads).</summary>
public sealed class SystemTextJsonSerializer<T> : IAsyncSerializer<T>
{
    public Task<byte[]> SerializeAsync(T data, SerializationContext context) =>
        Task.FromResult(JsonSerializer.SerializeToUtf8Bytes(data, KafkaJson.Options));
}

/// <summary>System.Text.Json deserializer with poison-message tolerance (returns null on bad payload).</summary>
public sealed class SystemTextJsonDeserializer<T> : IAsyncDeserializer<T>
{
    public Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull) return Task.FromResult(default(T)!);
        try
        {
            return Task.FromResult(JsonSerializer.Deserialize<T>(data.Span, KafkaJson.Options)!);
        }
        catch (JsonException)
        {
            // Poison messages are routed to the DLQ by the consumer host before we get here
            // in most paths; keep deserialization total so a single bad record never kills a partition.
            return Task.FromResult(default(T)!);
        }
    }
}
