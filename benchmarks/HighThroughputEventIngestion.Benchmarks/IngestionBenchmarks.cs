using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using HighThroughputEventIngestion.Core.Extensions;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace HighThroughputEventIngestion.Benchmarks;

[InProcess]
[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class IngestionBenchmarks
{
    private IIngestionService _ingestionService = null!;
    private IDeviceStore _deviceStore = null!;

    private Reading[] _batch100 = null!;
    private Reading[] _batch1000 = null!;
    private Reading[] _batch10000 = null!;
    private Reading[] _batch50000InOrder = null!;
    private Reading[] _batch50000OutOfOrder = null!;

    private byte[] _jsonBytes50000 = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddEventIngestionCore(options =>
        {
            options.MaxBatchSize = 100_000;
            options.ClockSkewFutureTolerance = TimeSpan.FromMinutes(1);
        });

        var provider = services.BuildServiceProvider();
        _ingestionService = provider.GetRequiredService<IIngestionService>();
        _deviceStore = provider.GetRequiredService<IDeviceStore>();

        var baseTime = DateTimeOffset.UtcNow;

        _batch100 = GenerateBatch(100, 10, baseTime, jitterSeconds: 0);
        _batch1000 = GenerateBatch(1_000, 100, baseTime, jitterSeconds: 0);
        _batch10000 = GenerateBatch(10_000, 1_000, baseTime, jitterSeconds: 0);
        _batch50000InOrder = GenerateBatch(50_000, 5_000, baseTime, jitterSeconds: 0);
        _batch50000OutOfOrder = GenerateBatch(50_000, 5_000, baseTime, jitterSeconds: 3);

        // Pre-serialize 50,000 readings to UTF-8 bytes to isolate streaming JSON ingestion without measuring serialization
        _jsonBytes50000 = JsonSerializer.SerializeToUtf8Bytes(_batch50000InOrder);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _deviceStore.Clear();
    }

    private static Reading[] GenerateBatch(int count, int deviceCount, DateTimeOffset baseTime, int jitterSeconds)
    {
        var readings = new Reading[count];
        // Generate timestamps strictly in the past (within the last 4 minutes)
        var start = baseTime.AddMinutes(-4);

        for (int i = 0; i < count; i++)
        {
            string deviceId = $"sensor-{i % deviceCount:D4}";
            double progressSeconds = (i / (double)count) * 200.0;
            int jitter = 0;
            if (jitterSeconds > 0)
            {
                jitter = (i % 7 == 0) ? -jitterSeconds : (i % 3 == 0) ? jitterSeconds : 0;
            }

            var ts = start.AddSeconds(progressSeconds + jitter);
            if (ts > baseTime) ts = baseTime;

            double val = 20.0 + (i % 50) * 0.25;
            readings[i] = new Reading(deviceId, ts, val);
        }
        return readings;
    }

    [Benchmark(Description = "Ingest (100 in-order)")]
    public IngestionResult Ingest_100_InOrder()
    {
        return _ingestionService.IngestBatch(_batch100);
    }

    [Benchmark(Description = "Ingest (1,000 in-order)")]
    public IngestionResult Ingest_1000_InOrder()
    {
        return _ingestionService.IngestBatch(_batch1000);
    }

    [Benchmark(Description = "Ingest (10,000 in-order)")]
    public IngestionResult Ingest_10000_InOrder()
    {
        return _ingestionService.IngestBatch(_batch10000);
    }

    [Benchmark(Description = "Ingest (50,000 in-order)")]
    public IngestionResult Ingest_50000_InOrder()
    {
        return _ingestionService.IngestBatch(_batch50000InOrder);
    }

    [Benchmark(Description = "Ingest (50,000 out-of-order)")]
    public IngestionResult Ingest_50000_OutOfOrder()
    {
        return _ingestionService.IngestBatch(_batch50000OutOfOrder);
    }

    [Benchmark(Description = "Streaming JSON Ingest (50,000 readings)")]
    public async Task<IngestionResult> IngestStream_50000_StreamingJson()
    {
        using var stream = new MemoryStream(_jsonBytes50000, writable: false);
        return await _ingestionService.IngestAsync(stream);
    }
}
