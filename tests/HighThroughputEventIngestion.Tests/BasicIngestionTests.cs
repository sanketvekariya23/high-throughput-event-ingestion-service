using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class BasicIngestionTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly IDeviceStore _deviceStore;
    private readonly IngestionOptions _options;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;

    public BasicIngestionTests()
    {
        var startTime = new DateTimeOffset(2026, 10, 10, 12, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(startTime);
        _deviceStore = new ConcurrentDeviceStore();
        _options = new IngestionOptions();
        _ingestionService = new IngestionService(_deviceStore, _timeProvider, _options);
        _aggregateService = new AggregateService(_deviceStore, _timeProvider, _options);
    }

    [Fact]
    public void IngestBatch_EmptyBatch_ReturnsZeroAccepted()
    {
        // Act
        var result = _ingestionService.IngestBatch(Array.Empty<Reading>());

        // Assert
        Assert.Equal(0, result.AcceptedCount);
        Assert.False(_aggregateService.TryGetAggregate("device-1", out _));
    }

    [Fact]
    public void IngestBatch_OneReading_SucceedsAndCalculatesAggregate()
    {
        // Arrange
        var now = _timeProvider.GetUtcNow();
        var reading = new Reading("sensor-1", now, 42.5);

        // Act
        var result = _ingestionService.IngestBatch(new[] { reading });

        // Assert
        Assert.Equal(1, result.AcceptedCount);
        Assert.True(_aggregateService.TryGetAggregate("sensor-1", out var aggregate));
        Assert.Equal("sensor-1", aggregate.DeviceId);
        Assert.Equal(1, aggregate.Count);
        Assert.Equal(42.5, aggregate.Min);
        Assert.Equal(42.5, aggregate.Max);
        Assert.Equal(42.5, aggregate.Average);
    }

    [Fact]
    public void IngestBatch_MultipleReadingsSameDevice_CalculatesAggregatesCorrectly()
    {
        // Arrange
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("sensor-1", now.AddSeconds(10), 10.0),
            new Reading("sensor-1", now.AddSeconds(20), 30.0),
            new Reading("sensor-1", now.AddSeconds(30), 20.0),
        };

        // Act
        var result = _ingestionService.IngestBatch(readings);

        // Assert
        Assert.Equal(3, result.AcceptedCount);

        _timeProvider.SetUtcNow(now.AddSeconds(30));
        Assert.True(_aggregateService.TryGetAggregate("sensor-1", out var aggregate));
        Assert.Equal(3, aggregate.Count);
        Assert.Equal(10.0, aggregate.Min);
        Assert.Equal(30.0, aggregate.Max);
        Assert.Equal(20.0, aggregate.Average);
    }

    [Fact]
    public void IngestBatch_MultipleDevices_MaintainsIndependentAggregates()
    {
        // Arrange
        var now = _timeProvider.GetUtcNow();
        var readings = new[]
        {
            new Reading("dev-A", now.AddSeconds(5), 100.0),
            new Reading("dev-B", now.AddSeconds(10), 200.0),
            new Reading("dev-A", now.AddSeconds(15), 150.0),
            new Reading("dev-B", now.AddSeconds(20), 250.0),
        };

        // Act
        var result = _ingestionService.IngestBatch(readings);

        // Assert
        Assert.Equal(4, result.AcceptedCount);

        _timeProvider.SetUtcNow(now.AddSeconds(20));

        Assert.True(_aggregateService.TryGetAggregate("dev-A", out var aggA));
        Assert.Equal(2, aggA.Count);
        Assert.Equal(100.0, aggA.Min);
        Assert.Equal(150.0, aggA.Max);
        Assert.Equal(125.0, aggA.Average);

        Assert.True(_aggregateService.TryGetAggregate("dev-B", out var aggB));
        Assert.Equal(2, aggB.Count);
        Assert.Equal(200.0, aggB.Min);
        Assert.Equal(250.0, aggB.Max);
        Assert.Equal(225.0, aggB.Average);
    }

    [Fact]
    public void IngestBatch_ExactlyFiftyThousandReadings_Succeeds()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new Reading[50_000];
        for (int i = 0; i < 50_000; i++)
        {
            readings[i] = new Reading($"dev-{i % 100}", now, 10.0);
        }

        var result = _ingestionService.IngestBatch(readings);

        Assert.Equal(50_000, result.AcceptedCount);
    }

    [Fact]
    public void IngestBatch_FiftyThousandAndOneReadings_ThrowsBatchSizeExceededException()
    {
        var now = _timeProvider.GetUtcNow();
        var readings = new Reading[50_001];
        for (int i = 0; i < 50_001; i++)
        {
            readings[i] = new Reading($"dev-{i % 100}", now, 10.0);
        }

        Assert.Throws<HighThroughputEventIngestion.Core.Exceptions.BatchSizeExceededException>(() =>
        {
            _ingestionService.IngestBatch(readings);
        });
    }
}
