using System.Text.Json.Serialization;

namespace HighThroughputEventIngestion.Core.Model;

/// <summary>
/// Represents the result of an event ingestion request.
/// </summary>
public readonly record struct IngestionResult(
    [property: JsonPropertyName("acceptedCount")] int AcceptedCount
);
