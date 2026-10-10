namespace HighThroughputEventIngestion.Core.Configuration;

/// <summary>
/// Configuration options for the high-throughput event ingestion service.
/// </summary>
public sealed class IngestionOptions
{
    /// <summary>
    /// Active sliding window duration (default: 5 minutes).
    /// </summary>
    public TimeSpan WindowDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum allowed number of readings per ingestion batch (default: 50,000).
    /// </summary>
    public int MaxBatchSize { get; set; } = 50_000;

    /// <summary>
    /// Threshold after which an idle device with no active readings is eligible for memory cleanup (default: 10 minutes).
    /// </summary>
    public TimeSpan InactiveDeviceThreshold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Maximum allowable future timestamp clock skew before rejection (default: 1 minute).
    /// </summary>
    public TimeSpan ClockSkewFutureTolerance { get; set; } = TimeSpan.FromMinutes(1);
}
