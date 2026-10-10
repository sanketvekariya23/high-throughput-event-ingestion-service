using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class ConcurrencyTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly ConcurrentDeviceStore _deviceStore;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;
    private readonly DateTimeOffset _baseTime;

    public ConcurrencyTests()
    {
        _baseTime = new DateTimeOffset(2026, 10, 10, 12, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(_baseTime);
        _deviceStore = new ConcurrentDeviceStore();
        var options = new IngestionOptions { WindowDuration = TimeSpan.FromMinutes(5) };
        _ingestionService = new IngestionService(_deviceStore, _timeProvider, options);
        _aggregateService = new AggregateService(_deviceStore, _timeProvider, options);
    }

    [Fact]
    public async Task ConcurrentIngestion_SameDevice_NoLostUpdates()
    {
        const int threadCount = 10;
        const int readingsPerThread = 500;
        const int totalExpected = threadCount * readingsPerThread;

        var startBarrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t;
            tasks[t] = Task.Run(() =>
            {
                var readings = new Reading[readingsPerThread];
                for (int i = 0; i < readingsPerThread; i++)
                {
                    // Stagger timestamps within 1 minute
                    var ts = _baseTime.AddMilliseconds((threadId * readingsPerThread + i) * 10);
                    readings[i] = new Reading("dev-concurrent-same", ts, 1.0);
                }

                startBarrier.SignalAndWait();
                _ingestionService.IngestBatch(readings);
            });
        }

        await Task.WhenAll(tasks);

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));

        Assert.True(_aggregateService.TryGetAggregate("dev-concurrent-same", out var agg));
        Assert.Equal(totalExpected, agg.Count);
        Assert.Equal(1.0, agg.Min);
        Assert.Equal(1.0, agg.Max);
        Assert.Equal(1.0, agg.Average);
    }

    [Fact]
    public async Task LargeConcurrentIngestion_TwentyThousandReadings_AllAggregatedAccurately()
    {
        const int threadCount = 20;
        const int readingsPerThread = 1_000;
        const int totalExpected = threadCount * readingsPerThread;

        var startBarrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t;
            tasks[t] = Task.Run(() =>
            {
                var readings = new Reading[readingsPerThread];
                for (int i = 0; i < readingsPerThread; i++)
                {
                    var ts = _baseTime.AddMilliseconds((threadId * readingsPerThread + i) * 2);
                    readings[i] = new Reading("dev-large-concurrent", ts, 5.0);
                }

                startBarrier.SignalAndWait();
                _ingestionService.IngestBatch(readings);
            });
        }

        await Task.WhenAll(tasks);

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));

        Assert.True(_aggregateService.TryGetAggregate("dev-large-concurrent", out var agg));
        Assert.Equal(totalExpected, agg.Count);
        Assert.Equal(5.0, agg.Min);
        Assert.Equal(5.0, agg.Max);
        Assert.Equal(5.0, agg.Average);
    }

    [Fact]
    public async Task ConcurrentIngestion_MultipleDevices_AllProcessedIndependently()
    {
        const int deviceCount = 20;
        const int readingsPerDevice = 200;

        var startBarrier = new Barrier(deviceCount);
        var tasks = new Task[deviceCount];

        for (int d = 0; d < deviceCount; d++)
        {
            int deviceIndex = d;
            string deviceId = $"dev-multi-{deviceIndex}";
            tasks[d] = Task.Run(() =>
            {
                var readings = new Reading[readingsPerDevice];
                for (int i = 0; i < readingsPerDevice; i++)
                {
                    var ts = _baseTime.AddMilliseconds(i * 100);
                    readings[i] = new Reading(deviceId, ts, (deviceIndex + 1) * 10.0);
                }

                startBarrier.SignalAndWait();
                _ingestionService.IngestBatch(readings);
            });
        }

        await Task.WhenAll(tasks);

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));

        for (int d = 0; d < deviceCount; d++)
        {
            string deviceId = $"dev-multi-{d}";
            Assert.True(_aggregateService.TryGetAggregate(deviceId, out var agg));
            Assert.Equal(readingsPerDevice, agg.Count);
            Assert.Equal((d + 1) * 10.0, agg.Min);
            Assert.Equal((d + 1) * 10.0, agg.Max);
            Assert.Equal((d + 1) * 10.0, agg.Average);
        }
    }

    [Fact]
    public async Task ConcurrentIngestionAndQuerying_ProducesConsistentSnapshotsWithoutTearing()
    {
        const string deviceId = "dev-race-test";
        const int writerCount = 4;
        const int writesPerThread = 500;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var startBarrier = new Barrier(writerCount + 2); // 4 writers + 2 readers
        var writerTasks = new Task[writerCount];

        for (int w = 0; w < writerCount; w++)
        {
            int writerId = w;
            writerTasks[w] = Task.Run(() =>
            {
                startBarrier.SignalAndWait();
                for (int i = 0; i < writesPerThread; i++)
                {
                    var ts = _baseTime.AddMilliseconds((writerId * writesPerThread + i) * 5);
                    double val = (i % 50) + 10.0;
                    _ingestionService.IngestBatch(new[] { new Reading(deviceId, ts, val) });
                }
            });
        }

        var readerExceptions = new List<Exception>();
        var readerTasks = new Task[2];

        for (int r = 0; r < 2; r++)
        {
            readerTasks[r] = Task.Run(() =>
            {
                startBarrier.SignalAndWait();
                while (!cts.Token.IsCancellationRequested)
                {
                    if (_aggregateService.TryGetAggregate(deviceId, out var agg))
                    {
                        try
                        {
                            Assert.True(agg.Count > 0, "Count must be > 0");
                            Assert.True(agg.Min <= agg.Max, $"Min ({agg.Min}) must be <= Max ({agg.Max})");
                            Assert.True(agg.Average >= agg.Min && agg.Average <= agg.Max,
                                $"Average ({agg.Average}) must be between Min ({agg.Min}) and Max ({agg.Max})");
                        }
                        catch (Exception ex)
                        {
                            lock (readerExceptions)
                            {
                                readerExceptions.Add(ex);
                            }
                        }
                    }
                    Thread.Yield();
                }
            });
        }

        await Task.WhenAll(writerTasks);
        cts.Cancel();
        await Task.WhenAll(readerTasks);

        Assert.Empty(readerExceptions);
    }
}
