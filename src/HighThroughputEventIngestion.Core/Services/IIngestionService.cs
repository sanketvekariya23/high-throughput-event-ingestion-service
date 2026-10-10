using HighThroughputEventIngestion.Core.Model;

namespace HighThroughputEventIngestion.Core.Services;

/// <summary>
/// Service responsible for high-throughput ingestion of device readings.
/// Supports streaming request processing without materializing entire batches.
/// </summary>
public interface IIngestionService
{
    /// <summary>
    /// Ingests readings from a UTF-8 JSON stream asynchronously.
    /// Streams readings individually to minimize memory allocations.
    /// </summary>
    ValueTask<IngestionResult> IngestAsync(Stream utf8JsonStream, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ingests readings from an asynchronous sequence.
    /// </summary>
    ValueTask<IngestionResult> IngestAsync(IAsyncEnumerable<Reading> readings, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ingests a collection of readings synchronously.
    /// </summary>
    IngestionResult IngestBatch(IEnumerable<Reading> readings);
}
