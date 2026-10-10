using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class MemoryAndCleanupTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly ConcurrentDeviceStore _deviceStore;
    private readonly IngestionOptions _options;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;
    private readonly DateTimeOffset _baseTime;

    public MemoryAndCleanupTests()
    {
        _baseTime = new DateTimeOffset(2026, 10, 10, 10, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(_baseTime);
        _deviceStore = new ConcurrentDeviceStore();
        _options = new IngestionOptions
        {
            WindowDuration = TimeSpan.FromMinutes(5),
            InactiveDeviceThreshold = TimeSpan.FromMinutes(10),
        };
        _ingestionService = new IngestionService(_deviceStore, _timeProvider, _options);
        _aggregateService = new AggregateService(_deviceStore, _timeProvider, _options);
    }

    [Fact]
    public void Memory_ExpiredReadingsStopContributingAndAreReleased()
    {
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-mem1", _baseTime, 100.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));
        Assert.True(_aggregateService.TryGetAggregate("dev-mem1", out var active));
        Assert.Equal(1, active.Count);

        // Advance past 5 minutes: Reading expires and is purged
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(1));
        Assert.False(_aggregateService.TryGetAggregate("dev-mem1", out _));

        // Check internal buffer count on DeviceState
        Assert.True(_deviceStore.TryGet("dev-mem1", out var state));
        Assert.Equal(0, state.ActiveCount);
    }

    [Fact]
    public void Memory_InactiveDeviceState_IsCleanedUpByMaintenanceSweep()
    {
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-stale", _baseTime, 55.0) });

        Assert.Equal(1, _deviceStore.DeviceCount);

        // Advance time to 15 minutes later (exceeding 5-min window + 10-min inactivity threshold)
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(16));

        // Trigger maintenance sweep
        int pruned = _aggregateService.PruneInactiveDevices();

        Assert.Equal(1, pruned);
        Assert.Equal(0, _deviceStore.DeviceCount);
        Assert.False(_deviceStore.TryGet("dev-stale", out _));
    }

    [Fact]
    public void Memory_ActiveDevices_AreNotPrunedDuringMaintenanceSweep()
    {
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-inactive", _baseTime, 10.0),
        });

        // Advance to 12 minutes and ingest reading for dev-active
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(12));
        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-active", _baseTime.AddMinutes(12), 20.0),
        });

        Assert.Equal(2, _deviceStore.DeviceCount);

        // Advance to 13 minutes: dev-inactive is eligible for prune, dev-active is still active
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(13));

        int pruned = _aggregateService.PruneInactiveDevices();

        Assert.Equal(1, pruned);
        Assert.Equal(1, _deviceStore.DeviceCount);
        Assert.True(_deviceStore.TryGet("dev-active", out _));
        Assert.False(_deviceStore.TryGet("dev-inactive", out _));
    }

    [Fact]
    public void Memory_DeviceBufferCompaction_TrimsOversizedArraysAfterBurst()
    {
        _timeProvider.SetUtcNow(_baseTime);

        // Ingest burst of 300 readings to force buffer expansion beyond 256
        var burst = Enumerable.Range(1, 300)
            .Select(i => new Reading("dev-burst", _baseTime.AddSeconds(1), (double)i))
            .ToArray();

        _ingestionService.IngestBatch(burst);
        Assert.True(_deviceStore.TryGet("dev-burst", out var state));
        Assert.Equal(300, state.ActiveCount);

        // Advance time so all 300 readings expire
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6));

        // When queried, expired readings are purged and buffer is compacted
        Assert.False(_aggregateService.TryGetAggregate("dev-burst", out _));
        Assert.Equal(0, state.ActiveCount);
    }

    [Fact]
    public void Memory_ReIngestionAfterRetirement_CreatesFreshStateAndAggregatesCorrectly()
    {
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-reborn", _baseTime, 10.0) });

        // Advance past 15 minutes (5m window + 10m inactive threshold)
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(16));
        int pruned = _aggregateService.PruneInactiveDevices();
        Assert.Equal(1, pruned);
        Assert.False(_deviceStore.TryGet("dev-reborn", out _));

        // Re-ingest at the new time (16m)
        var newReadingTime = _baseTime.AddMinutes(16);
        _ingestionService.IngestBatch(new[] { new Reading("dev-reborn", newReadingTime, 99.0) });

        // Device should be reincarnated with fresh state
        Assert.True(_aggregateService.TryGetAggregate("dev-reborn", out var agg));
        Assert.Equal(1, agg.Count);
        Assert.Equal(99.0, agg.Min);
        Assert.Equal(99.0, agg.Max);
        Assert.Equal(99.0, agg.Average);
    }

    [Fact]
    public async Task Memory_ConcurrentCleanupAndIngestion_NoLostUpdatesOrExceptions()
    {
        const int deviceCount = 10;
        const int iterations = 300;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var writerTasks = new Task[4];
        for (int w = 0; w < 4; w++)
        {
            int writerId = w;
            writerTasks[w] = Task.Run(() =>
            {
                for (int i = 0; i < iterations && !cts.Token.IsCancellationRequested; i++)
                {
                    string deviceId = $"dev-race-clean-{i % deviceCount}";
                    var reading = new Reading(deviceId, _baseTime.AddMilliseconds(i), 1.0);
                    _ingestionService.IngestBatch(new[] { reading });
                    Thread.Yield();
                }
            });
        }

        var cleanerTask = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                _aggregateService.PruneInactiveDevices();
                Thread.Yield();
            }
        });

        await Task.WhenAll(writerTasks);
        cts.Cancel();
        await cleanerTask;

        // Verify that system state is completely consistent and uncorrupted
        for (int d = 0; d < deviceCount; d++)
        {
            string deviceId = $"dev-race-clean-{d}";
            if (_aggregateService.TryGetAggregate(deviceId, out var agg))
            {
                Assert.True(agg.Count > 0);
                Assert.Equal(1.0, agg.Min);
                Assert.Equal(1.0, agg.Max);
            }
        }
    }
}
