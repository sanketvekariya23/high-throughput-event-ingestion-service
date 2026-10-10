using System.Text.Json.Serialization;

namespace HighThroughputEventIngestion.Core.Model;

/// <summary>
/// Represents an incoming IoT device reading.
/// </summary>
public readonly record struct Reading(
    [property: JsonPropertyName("deviceId")] string? DeviceId,
    [property: JsonPropertyName("timestamp")] DateTimeOffset Timestamp,
    [property: JsonPropertyName("value")] double Value
);
