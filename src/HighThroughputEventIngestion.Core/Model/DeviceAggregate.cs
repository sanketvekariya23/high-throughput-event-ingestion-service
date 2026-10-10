using System.Text.Json.Serialization;

namespace HighThroughputEventIngestion.Core.Model;

/// <summary>
/// Represents the rolling aggregate summary for a specific device over the active time window.
/// </summary>
public readonly record struct DeviceAggregate(
    [property: JsonPropertyName("deviceId")] string DeviceId,
    [property: JsonPropertyName("count")] long Count,
    [property: JsonPropertyName("min")] double Min,
    [property: JsonPropertyName("max")] double Max,
    [property: JsonPropertyName("average")] double Average
);
