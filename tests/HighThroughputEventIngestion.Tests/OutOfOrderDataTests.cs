using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class OutOfOrderDataTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;
    private readonly DateTimeOffset _baseTime;

    public OutOfOrderDataTests()
    {
        _baseTime = new DateTimeOffset(2026, 10, 10, 10, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(_baseTime);
        var deviceStore = new ConcurrentDeviceStore();
        var options = new IngestionOptions { WindowDuration = TimeSpan.FromMinutes(5) };
        _ingestionService = new IngestionService(deviceStore, _timeProvider, options);
        _aggregateService = new AggregateService(deviceStore, _timeProvider, options);
    }

    [Fact]
    public void OutOfOrder_SingleOutOfOrderReading_MaintainsChronologicalWindow()
    {
        // Reading A: 10:00:10 (value 20)
        // Reading B: 10:00:05 (value 10) - arrives later than Reading A
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(10));

        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-ooo1", _baseTime.AddSeconds(10), 20.0),
            new Reading("dev-ooo1", _baseTime.AddSeconds(5), 10.0),
        });

        Assert.True(_aggregateService.TryGetAggregate("dev-ooo1", out var agg));
        Assert.Equal(2, agg.Count);
        Assert.Equal(10.0, agg.Min);
        Assert.Equal(20.0, agg.Max);
        Assert.Equal(15.0, agg.Average);

        // When time advances to 10:05:06, Reading B (10:00:05) should expire first, leaving Reading A (10:00:10)
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(6));
        Assert.True(_aggregateService.TryGetAggregate("dev-ooo1", out var aggAfterB));
        Assert.Equal(1, aggAfterB.Count);
        Assert.Equal(20.0, aggAfterB.Min);
        Assert.Equal(20.0, aggAfterB.Max);
        Assert.Equal(20.0, aggAfterB.Average);
    }

    [Fact]
    public void OutOfOrder_MultipleOutOfOrderReadings_InsertedInCorrectOrder()
    {
        // The prompt's example:
        // Reading A: 10:00:10 (val 30)
        // Reading B: 10:00:05 (val 10)
        // Reading C: 10:00:08 (val 20)
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(10));

        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-ooo2", _baseTime.AddSeconds(10), 30.0),
            new Reading("dev-ooo2", _baseTime.AddSeconds(5), 10.0),
            new Reading("dev-ooo2", _baseTime.AddSeconds(8), 20.0),
        });

        Assert.True(_aggregateService.TryGetAggregate("dev-ooo2", out var agg));
        Assert.Equal(3, agg.Count);
        Assert.Equal(10.0, agg.Min);
        Assert.Equal(30.0, agg.Max);
        Assert.Equal(20.0, agg.Average);

        // Advance to 10:05:06: Reading B (05s) expires. C (08s) and A (10s) remain.
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(6));
        Assert.True(_aggregateService.TryGetAggregate("dev-ooo2", out var agg1));
        Assert.Equal(2, agg1.Count);
        Assert.Equal(20.0, agg1.Min);
        Assert.Equal(30.0, agg1.Max);
        Assert.Equal(25.0, agg1.Average);

        // Advance to 10:05:09: Reading C (08s) expires. Only A (10s) remains.
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(9));
        Assert.True(_aggregateService.TryGetAggregate("dev-ooo2", out var agg2));
        Assert.Equal(1, agg2.Count);
        Assert.Equal(30.0, agg2.Min);
        Assert.Equal(30.0, agg2.Max);
        Assert.Equal(30.0, agg2.Average);
    }

    [Fact]
    public void OutOfOrder_LateReadingAlreadyOutsideWindow_DoesNotContributeToActiveAggregate()
    {
        // Current time: 10:06:00. Cutoff: 10:01:00.
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6));

        // Ingest reading with timestamp 10:00:00 (6 minutes old, already outside 5-minute window)
        var lateReading = new Reading("dev-late", _baseTime, 99.0);
        var activeReading = new Reading("dev-late", _baseTime.AddMinutes(4), 10.0);

        _ingestionService.IngestBatch(new[] { lateReading, activeReading });

        // Only the active reading should contribute
        Assert.True(_aggregateService.TryGetAggregate("dev-late", out var agg));
        Assert.Equal(1, agg.Count);
        Assert.Equal(10.0, agg.Min);
        Assert.Equal(10.0, agg.Max);
        Assert.Equal(10.0, agg.Average);
    }

    [Fact]
    public void OutOfOrder_ReadingAffectingMinAndMax_UpdatesAggregatesCorrectly()
    {
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(20));

        // Initial reading at 20s with value 50.0
        _ingestionService.IngestBatch(new[] { new Reading("dev-minmax", _baseTime.AddSeconds(20), 50.0) });

        // Out-of-order reading at 10s with value 5.0 (new minimum)
        _ingestionService.IngestBatch(new[] { new Reading("dev-minmax", _baseTime.AddSeconds(10), 5.0) });

        Assert.True(_aggregateService.TryGetAggregate("dev-minmax", out var aggMin));
        Assert.Equal(2, aggMin.Count);
        Assert.Equal(5.0, aggMin.Min);
        Assert.Equal(50.0, aggMin.Max);

        // Out-of-order reading at 15s with value 95.0 (new maximum)
        _ingestionService.IngestBatch(new[] { new Reading("dev-minmax", _baseTime.AddSeconds(15), 95.0) });

        Assert.True(_aggregateService.TryGetAggregate("dev-minmax", out var aggMax));
        Assert.Equal(3, aggMax.Count);
        Assert.Equal(5.0, aggMax.Min);
        Assert.Equal(95.0, aggMax.Max);
    }

    [Fact]
    public void OutOfOrder_ReadingAffectingAverage_UpdatesAverageAccurately()
    {
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(30));

        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-avg-ooo", _baseTime.AddSeconds(30), 10.0),
            new Reading("dev-avg-ooo", _baseTime.AddSeconds(10), 30.0),
            new Reading("dev-avg-ooo", _baseTime.AddSeconds(20), 50.0),
        });

        Assert.True(_aggregateService.TryGetAggregate("dev-avg-ooo", out var agg));
        Assert.Equal(3, agg.Count);
        Assert.Equal((10.0 + 30.0 + 50.0) / 3.0, agg.Average);
    }
}
