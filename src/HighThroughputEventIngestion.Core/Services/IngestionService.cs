using System.Text.Json;
using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Exceptions;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Storage;
using HighThroughputEventIngestion.Core.Validation;

namespace HighThroughputEventIngestion.Core.Services;

/// <summary>
/// High-throughput streaming event ingestion service.
/// Streams readings directly without materializing full request batches in memory.
/// </summary>
internal sealed class IngestionService : IIngestionService
{
    private static readonly JsonSerializerOptions DefaultJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly IDeviceStore _deviceStore;
    private readonly TimeProvider _timeProvider;
    private readonly IngestionOptions _options;

    public IngestionService(
        IDeviceStore deviceStore,
        TimeProvider timeProvider,
        IngestionOptions? options = null)
    {
        _deviceStore = deviceStore ?? throw new ArgumentNullException(nameof(deviceStore));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _options = options ?? new IngestionOptions();
    }

    public async ValueTask<IngestionResult> IngestAsync(Stream utf8JsonStream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(utf8JsonStream);

        var asyncEnumerable = JsonSerializer.DeserializeAsyncEnumerable<Reading>(
            utf8JsonStream,
            DefaultJsonOptions,
            cancellationToken);

        return await IngestAsync(asyncEnumerable, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<IngestionResult> IngestAsync(IAsyncEnumerable<Reading> readings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(readings);

        int count = 0;
        DateTimeOffset now = _timeProvider.GetUtcNow();
        long nowTicks = now.UtcTicks;
        long cutoffTicks = nowTicks - _options.WindowDuration.Ticks;

        await foreach (var reading in readings.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            count++;
            if (count > _options.MaxBatchSize)
            {
                throw new BatchSizeExceededException($"Batch size exceeds the maximum allowed limit of {_options.MaxBatchSize} readings.");
            }

            ReadingValidator.Validate(reading, _options.ClockSkewFutureTolerance, now);
            AddReadingToStore(reading, cutoffTicks, nowTicks);
        }

        return new IngestionResult(count);
    }

    public IngestionResult IngestBatch(IEnumerable<Reading> readings)
    {
        ArgumentNullException.ThrowIfNull(readings);

        int count = 0;
        DateTimeOffset now = _timeProvider.GetUtcNow();
        long nowTicks = now.UtcTicks;
        long cutoffTicks = nowTicks - _options.WindowDuration.Ticks;

        foreach (var reading in readings)
        {
            count++;
            if (count > _options.MaxBatchSize)
            {
                throw new BatchSizeExceededException($"Batch size exceeds the maximum allowed limit of {_options.MaxBatchSize} readings.");
            }

            ReadingValidator.Validate(reading, _options.ClockSkewFutureTolerance, now);
            AddReadingToStore(reading, cutoffTicks, nowTicks);
        }

        return new IngestionResult(count);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private void AddReadingToStore(in Reading reading, long cutoffTicks, long nowTicks)
    {
        string deviceId = reading.DeviceId!;
        while (true)
        {
            var deviceState = _deviceStore.GetOrCreate(deviceId);
            if (deviceState.AddReading(reading.Timestamp, reading.Value, cutoffTicks, nowTicks))
            {
                break;
            }
            // State was retired concurrently; retry with fresh instance
        }
    }
}
