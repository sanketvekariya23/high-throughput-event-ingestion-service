using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using HighThroughputEventIngestion.Tests.Fakes;

namespace HighThroughputEventIngestion.Tests;

public class SlidingWindowTests
{
    private readonly FakeTimeProvider _timeProvider;
    private readonly IngestionService _ingestionService;
    private readonly AggregateService _aggregateService;
    private readonly DateTimeOffset _baseTime;

    public SlidingWindowTests()
    {
        _baseTime = new DateTimeOffset(2026, 10, 10, 12, 0, 0, TimeSpan.Zero);
        _timeProvider = new FakeTimeProvider(_baseTime);
        var deviceStore = new ConcurrentDeviceStore();
        var options = new IngestionOptions { WindowDuration = TimeSpan.FromMinutes(5) };
        _ingestionService = new IngestionService(deviceStore, _timeProvider, options);
        _aggregateService = new AggregateService(deviceStore, _timeProvider, options);
    }

    [Fact]
    public void SlidingWindow_ReadingInsideFiveMinuteWindow_ContributesToAggregate()
    {
        // Window: (T - 5 min, T]
        // Ingest at baseTime + 1 min, query at baseTime + 4 min
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(1));
        _ingestionService.IngestBatch(new[] { new Reading("dev-w1", _baseTime.AddMinutes(1), 50.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));
        Assert.True(_aggregateService.TryGetAggregate("dev-w1", out var agg));
        Assert.Equal(1, agg.Count);
        Assert.Equal(50.0, agg.Average);
    }

    [Fact]
    public void SlidingWindow_ReadingExactlyAtBoundary_IsExpiredAndDoesNotContribute()
    {
        // Convention: (T - 5 minutes, T]
        // If query is at 12:05:00, cutoff is 12:00:00.
        // A reading at exactly 12:00:00 (baseTime) is at the exact boundary T - 5min.
        // Because the lower bound is open (T - 5 min, ...), 12:00:00 is EXPIRED.
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-bound", _baseTime, 10.0) });

        // Advance to exactly baseTime + 5 minutes
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5));

        Assert.False(_aggregateService.TryGetAggregate("dev-bound", out _));
    }

    [Fact]
    public void SlidingWindow_ReadingJustInsideBoundary_ContributesToAggregate()
    {
        // Reading timestamp is 1 tick after baseTime: baseTime + 1 millisecond
        // At query time baseTime + 5 minutes:
        // Cutoff is baseTime. Reading timestamp (baseTime + 1ms) > baseTime.
        // Therefore it is INSIDE the window and contributes!
        var readingTime = _baseTime.AddMilliseconds(1);
        _timeProvider.SetUtcNow(readingTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-bound-in", readingTime, 10.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5));

        Assert.True(_aggregateService.TryGetAggregate("dev-bound-in", out var agg));
        Assert.Equal(1, agg.Count);
        Assert.Equal(10.0, agg.Average);
    }

    [Fact]
    public void SlidingWindow_ReadingJustOutsideBoundary_IsExpiredAndDoesNotContribute()
    {
        // Reading timestamp is 1 millisecond before baseTime: baseTime - 1ms
        // Query at baseTime + 4m 59s 999ms (or 5 minutes)
        // Cutoff at baseTime + 5 min is baseTime.
        // Reading is at baseTime - 1ms <= baseTime, so it is outside.
        var readingTime = _baseTime.AddMilliseconds(-1);
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-bound-out", readingTime, 10.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5));

        Assert.False(_aggregateService.TryGetAggregate("dev-bound-out", out _));
    }

    [Fact]
    public void SlidingWindow_ExpiredReadingIsRemoved_WhenTimeAdvances()
    {
        // Reading 1 at 12:00:30 (value 10)
        // Reading 2 at 12:01:00 (value 20)
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(30));
        _ingestionService.IngestBatch(new[] { new Reading("dev-adv", _baseTime.AddSeconds(30), 10.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(1));
        _ingestionService.IngestBatch(new[] { new Reading("dev-adv", _baseTime.AddMinutes(1), 20.0) });

        // Query at 12:05:00: both are within (12:00:00, 12:05:00]
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5));
        Assert.True(_aggregateService.TryGetAggregate("dev-adv", out var agg1));
        Assert.Equal(2, agg1.Count);
        Assert.Equal(15.0, agg1.Average);

        // Advance to 12:05:31: Reading 1 (12:00:30) is now <= 12:00:31, so it expires!
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(31));
        Assert.True(_aggregateService.TryGetAggregate("dev-adv", out var agg2));
        Assert.Equal(1, agg2.Count);
        Assert.Equal(20.0, agg2.Min);
        Assert.Equal(20.0, agg2.Max);
        Assert.Equal(20.0, agg2.Average);
    }

    [Fact]
    public void SlidingWindow_MultipleExpirations_MaintainAccurateAggregates()
    {
        // Ingest readings from minute 1 to minute 5
        for (int m = 1; m <= 5; m++)
        {
            var t = _baseTime.AddMinutes(m);
            _timeProvider.SetUtcNow(t);
            _ingestionService.IngestBatch(new[] { new Reading("dev-mult", t, m * 10.0) });
        }

        // At 12:05:00: readings at min 1, 2, 3, 4, 5 are active (10, 20, 30, 40, 50)
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5));
        Assert.True(_aggregateService.TryGetAggregate("dev-mult", out var aggInitial));
        Assert.Equal(5, aggInitial.Count);
        Assert.Equal(10.0, aggInitial.Min);
        Assert.Equal(50.0, aggInitial.Max);
        Assert.Equal(30.0, aggInitial.Average);

        // Advance to 12:07:30 (7.5 minutes).
        // Cutoff is 12:02:30. Readings at min 1 and 2 are expired!
        // Active readings: min 3 (30), min 4 (40), min 5 (50).
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(7).AddSeconds(30));
        Assert.True(_aggregateService.TryGetAggregate("dev-mult", out var aggExpired));
        Assert.Equal(3, aggExpired.Count);
        Assert.Equal(30.0, aggExpired.Min);
        Assert.Equal(50.0, aggExpired.Max);
        Assert.Equal(40.0, aggExpired.Average);
    }

    [Fact]
    public void SlidingWindow_ContinuouslyAdvances_AllReadingsEventuallyExpire()
    {
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(1));
        _ingestionService.IngestBatch(new[] { new Reading("dev-all-exp", _baseTime.AddMinutes(1), 100.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));
        Assert.True(_aggregateService.TryGetAggregate("dev-all-exp", out var activeAgg));
        Assert.Equal(1, activeAgg.Count);

        // Advance beyond 5 minutes from ingestion (6 minutes 1 second)
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6).AddSeconds(1));
        Assert.False(_aggregateService.TryGetAggregate("dev-all-exp", out _));
    }

    [Fact]
    public void SlidingWindow_MinimumReadingExpires_RecalculatesNewMinimumCorrectly()
    {
        // Reading 1 (t = 1m): value 5.0 (the minimum!)
        // Reading 2 (t = 2m): value 25.0
        // Reading 3 (t = 3m): value 15.0
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(1));
        _ingestionService.IngestBatch(new[] { new Reading("dev-min-exp", _baseTime.AddMinutes(1), 5.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(2));
        _ingestionService.IngestBatch(new[] { new Reading("dev-min-exp", _baseTime.AddMinutes(2), 25.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(3));
        _ingestionService.IngestBatch(new[] { new Reading("dev-min-exp", _baseTime.AddMinutes(3), 15.0) });

        // At t = 4m: min is 5.0
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));
        Assert.True(_aggregateService.TryGetAggregate("dev-min-exp", out var agg1));
        Assert.Equal(5.0, agg1.Min);

        // Advance to t = 6m 1s: Reading 1 (5.0) expires.
        // Remaining active readings: 25.0 and 15.0. New min must be 15.0!
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6).AddSeconds(1));
        Assert.True(_aggregateService.TryGetAggregate("dev-min-exp", out var agg2));
        Assert.Equal(2, agg2.Count);
        Assert.Equal(15.0, agg2.Min);
        Assert.Equal(25.0, agg2.Max);
        Assert.Equal(20.0, agg2.Average);
    }

    [Fact]
    public void SlidingWindow_MaximumReadingExpires_RecalculatesNewMaximumCorrectly()
    {
        // Reading 1 (t = 1m): value 100.0 (the maximum!)
        // Reading 2 (t = 2m): value 40.0
        // Reading 3 (t = 3m): value 70.0
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(1));
        _ingestionService.IngestBatch(new[] { new Reading("dev-max-exp", _baseTime.AddMinutes(1), 100.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(2));
        _ingestionService.IngestBatch(new[] { new Reading("dev-max-exp", _baseTime.AddMinutes(2), 40.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(3));
        _ingestionService.IngestBatch(new[] { new Reading("dev-max-exp", _baseTime.AddMinutes(3), 70.0) });

        // Advance to t = 6m 1s: Reading 1 (100.0) expires.
        // Remaining active readings: 40.0 and 70.0. New max must be 70.0!
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6).AddSeconds(1));
        Assert.True(_aggregateService.TryGetAggregate("dev-max-exp", out var agg));
        Assert.Equal(2, agg.Count);
        Assert.Equal(40.0, agg.Min);
        Assert.Equal(70.0, agg.Max);
        Assert.Equal(55.0, agg.Average);
    }

    [Fact]
    public void SlidingWindow_DuplicateTimestamps_BothContributeAndExpireTogether()
    {
        var t = _baseTime.AddMinutes(1);
        _timeProvider.SetUtcNow(t);

        _ingestionService.IngestBatch(new[]
        {
            new Reading("dev-dup-ts", t, 10.0),
            new Reading("dev-dup-ts", t, 20.0),
        });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(4));
        Assert.True(_aggregateService.TryGetAggregate("dev-dup-ts", out var agg));
        Assert.Equal(2, agg.Count);
        Assert.Equal(10.0, agg.Min);
        Assert.Equal(20.0, agg.Max);
        Assert.Equal(15.0, agg.Average);

        // Advance to 6m 1s: both readings at 1m should expire simultaneously
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6).AddSeconds(1));
        Assert.False(_aggregateService.TryGetAggregate("dev-dup-ts", out _));
    }

    [Fact]
    public void SlidingWindow_SimultaneousMinAndMaxExpiration_HandledCorrectly()
    {
        // Only one reading exists (it is simultaneously min and max)
        var t = _baseTime.AddMinutes(1);
        _timeProvider.SetUtcNow(t);
        _ingestionService.IngestBatch(new[] { new Reading("dev-single-minmax", t, 42.0) });

        _timeProvider.SetUtcNow(_baseTime.AddMinutes(2));
        Assert.True(_aggregateService.TryGetAggregate("dev-single-minmax", out var agg));
        Assert.Equal(1, agg.Count);
        Assert.Equal(42.0, agg.Min);
        Assert.Equal(42.0, agg.Max);

        // Advance past 5 minutes: the reading expires
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(6).AddSeconds(1));
        Assert.False(_aggregateService.TryGetAggregate("dev-single-minmax", out _));
    }

    [Fact]
    public void SlidingWindow_FutureTimestampWithinTolerance_BecomesActiveAsTimeAdvances()
    {
        // Clock is at baseTime. Reading arrives with timestamp baseTime + 30s (within 1-min tolerance).
        _timeProvider.SetUtcNow(_baseTime);
        _ingestionService.IngestBatch(new[] { new Reading("dev-future", _baseTime.AddSeconds(30), 88.0) });

        // Query immediately at baseTime: reading has timestamp > baseTime, so it is not yet active
        Assert.False(_aggregateService.TryGetAggregate("dev-future", out _));

        // Advance virtual clock to baseTime + 30s: reading is now active!
        _timeProvider.SetUtcNow(_baseTime.AddSeconds(30));
        Assert.True(_aggregateService.TryGetAggregate("dev-future", out var aggActive));
        Assert.Equal(1, aggActive.Count);
        Assert.Equal(88.0, aggActive.Average);

        // Advance to baseTime + 5m 31s: reading expires
        _timeProvider.SetUtcNow(_baseTime.AddMinutes(5).AddSeconds(31));
        Assert.False(_aggregateService.TryGetAggregate("dev-future", out _));
    }

    [Fact]
    public void SlidingWindow_FutureTimestampExceedingTolerance_ThrowsValidationException()
    {
        // Clock at baseTime. Reading timestamp is 2 minutes in future (exceeds 1-min tolerance).
        _timeProvider.SetUtcNow(_baseTime);

        Assert.Throws<HighThroughputEventIngestion.Core.Exceptions.ReadingValidationException>(() =>
        {
            _ingestionService.IngestBatch(new[] { new Reading("dev-too-future", _baseTime.AddMinutes(2), 50.0) });
        });
    }
}
