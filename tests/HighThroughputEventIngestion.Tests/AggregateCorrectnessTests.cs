using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class AggregateCorrectnessTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;

    public AggregateCorrectnessTests()
    {
        var baseTime = new DateTimeOffset(2026, 10, 10, 10, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(baseTime);
        var deviceStore = new ConcurrentDeviceStore();
        var options = new IngestionOptions();
        _ingestionService = new IngestionService(deviceStore, _timeProvider, options);
        _aggregateService = new AggregateService(deviceStore, _timeProvider, options);
    }

    [Fact]
    public void Aggregates_Count_MatchesExactNumberOfActiveReadings()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = Enumerable.Range(1, 100)
            .Select(i => new Reading("sensor-cnt", now.AddSeconds(-100 + i), i * 1.5))
            .ToArray();

        _ingestionService.IngestBatch(readings);

        Assert.True(_aggregateService.TryGetAggregate("sensor-cnt", out var agg));
        Assert.Equal(100, agg.Count);
    }

    [Fact]
    public void Aggregates_Minimum_AccuratelyTracksLowestValueIncludingNegatives()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("sensor-min", now.AddSeconds(1), 15.5),
            new Reading("sensor-min", now.AddSeconds(2), -4.2),
            new Reading("sensor-min", now.AddSeconds(3), 0.0),
            new Reading("sensor-min", now.AddSeconds(4), -10.8),
            new Reading("sensor-min", now.AddSeconds(5), 7.3),
        };

        _ingestionService.IngestBatch(readings);
        _timeProvider.SetUtcNow(now.AddSeconds(5));

        Assert.True(_aggregateService.TryGetAggregate("sensor-min", out var agg));
        Assert.Equal(-10.8, agg.Min);
    }

    [Fact]
    public void Aggregates_Maximum_AccuratelyTracksHighestValue()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("sensor-max", now.AddSeconds(1), -50.0),
            new Reading("sensor-max", now.AddSeconds(2), -10.0),
            new Reading("sensor-max", now.AddSeconds(3), -5.5),
            new Reading("sensor-max", now.AddSeconds(4), -20.0),
        };

        _ingestionService.IngestBatch(readings);
        _timeProvider.SetUtcNow(now.AddSeconds(4));

        Assert.True(_aggregateService.TryGetAggregate("sensor-max", out var agg));
        Assert.Equal(-5.5, agg.Max);
    }

    [Fact]
    public void Aggregates_Average_CalculatesArithmeticMeanAccurately()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("sensor-avg", now.AddSeconds(1), 10.0),
            new Reading("sensor-avg", now.AddSeconds(2), 20.0),
            new Reading("sensor-avg", now.AddSeconds(3), 30.0),
            new Reading("sensor-avg", now.AddSeconds(4), 40.0),
        };

        _ingestionService.IngestBatch(readings);
        _timeProvider.SetUtcNow(now.AddSeconds(4));

        Assert.True(_aggregateService.TryGetAggregate("sensor-avg", out var agg));
        Assert.Equal(25.0, agg.Average);
    }

    [Fact]
    public void Aggregates_DuplicateMinAndMaxValues_HandledCorrectly()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("sensor-dup", now.AddSeconds(1), 5.0),
            new Reading("sensor-dup", now.AddSeconds(2), 5.0),
            new Reading("sensor-dup", now.AddSeconds(3), 10.0),
            new Reading("sensor-dup", now.AddSeconds(4), 10.0),
        };

        _ingestionService.IngestBatch(readings);
        _timeProvider.SetUtcNow(now.AddSeconds(4));

        Assert.True(_aggregateService.TryGetAggregate("sensor-dup", out var agg));
        Assert.Equal(4, agg.Count);
        Assert.Equal(5.0, agg.Min);
        Assert.Equal(10.0, agg.Max);
        Assert.Equal(7.5, agg.Average);
    }
}
