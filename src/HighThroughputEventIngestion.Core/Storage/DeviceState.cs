using HighThroughputEventIngestion.Core.Model;

namespace HighThroughputEventIngestion.Core.Storage;

/// <summary>
/// Maintains rolling window aggregate state for a single device.
/// All mutations and snapshot reads are thread-safe and isolated per-device.
/// </summary>
internal sealed class DeviceState
{
    private const int InitialCapacity = 16;
    private const int CompactThresholdCapacity = 256;

    public string DeviceId { get; }

    private readonly object _gate = new();

    private TimestampedReading[] _buffer = new TimestampedReading[InitialCapacity];
    private int _head;
    private int _count;

    private double _min = double.PositiveInfinity;
    private double _max = double.NegativeInfinity;
    private int _minCount;
    private int _maxCount;
    private double _sum;

    private long _lastIngestTicks;
    private bool _isRetired;

    public DeviceState(string deviceId)
    {
        DeviceId = deviceId;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private int IndexAt(int offset) => (_head + offset) % _buffer.Length;

    /// <summary>
    /// Ingests a new reading for this device into the active window.
    /// Returns false if this instance has been retired concurrently, signaling the caller to retry with a fresh instance.
    /// </summary>
    public bool AddReading(DateTimeOffset timestamp, double value, long cutoffTicks, long nowTicks)
    {
        lock (_gate)
        {
            if (_isRetired)
            {
                return false;
            }

            long ticks = timestamp.UtcTicks;

            // Enforce sliding-window boundary: (now - window, now]
            // Any reading with ticks <= cutoffTicks is already expired relative to current cutoff
            if (ticks <= cutoffTicks)
            {
                // Expired on arrival; does not alter active aggregates
                _lastIngestTicks = Math.Max(_lastIngestTicks, nowTicks);
                return true;
            }

            // Expire any readings that have aged out
            ExpireOldReadingsInternal(cutoffTicks);

            // Ensure buffer has capacity for the new reading
            EnsureCapacityInternal();

            // Insert reading in chronological order (handling slightly out-of-order timestamps)
            InsertReadingInternal(ticks, value);

            _lastIngestTicks = Math.Max(_lastIngestTicks, nowTicks);
            return true;
        }
    }

    private void InsertReadingInternal(long ticks, double value)
    {
        if (_count == 0 || ticks >= _buffer[IndexAt(_count - 1)].Ticks)
        {
            // In-order append (fast common path: O(1))
            int tail = IndexAt(_count);
            _buffer[tail] = new TimestampedReading(ticks, value);
            _count++;
        }
        else
        {
            // Slightly out-of-order: search backwards to find insertion point
            int insertPos = _count;
            while (insertPos > 0 && _buffer[IndexAt(insertPos - 1)].Ticks > ticks)
            {
                insertPos--;
            }

            // Shift elements right by one to open slot at insertPos
            for (int i = _count - 1; i >= insertPos; i--)
            {
                _buffer[IndexAt(i + 1)] = _buffer[IndexAt(i)];
            }

            _buffer[IndexAt(insertPos)] = new TimestampedReading(ticks, value);
            _count++;
        }

        // Incrementally maintain aggregate values
        _sum += value;

        if (value < _min)
        {
            _min = value;
            _minCount = 1;
        }
        else if (value == _min)
        {
            _minCount++;
        }

        if (value > _max)
        {
            _max = value;
            _maxCount = 1;
        }
        else if (value == _max)
        {
            _maxCount++;
        }
    }

    private void ExpireOldReadingsInternal(long cutoffTicks)
    {
        if (_count == 0)
        {
            return;
        }

        bool minNeedsRescan = false;
        bool maxNeedsRescan = false;

        while (_count > 0 && _buffer[_head].Ticks <= cutoffTicks)
        {
            double val = _buffer[_head].Value;
            _head = (_head + 1) % _buffer.Length;
            _count--;
            _sum -= val;

            if (val == _min)
            {
                _minCount--;
                if (_minCount <= 0)
                {
                    minNeedsRescan = true;
                }
            }

            if (val == _max)
            {
                _maxCount--;
                if (_maxCount <= 0)
                {
                    maxNeedsRescan = true;
                }
            }
        }

        if (_count == 0)
        {
            _head = 0;
            _sum = 0.0;
            _min = double.PositiveInfinity;
            _max = double.NegativeInfinity;
            _minCount = 0;
            _maxCount = 0;
            return;
        }

        if (minNeedsRescan || maxNeedsRescan)
        {
            RescanMinMaxAndSumInternal();
        }

        // Shrink buffer if it was grown during a temporary burst and has significantly emptied
        if (_buffer.Length > CompactThresholdCapacity && _count < _buffer.Length / 4)
        {
            CompactBufferInternal();
        }
    }

    private void RescanMinMaxAndSumInternal()
    {
        double newMin = double.PositiveInfinity;
        double newMax = double.NegativeInfinity;
        int minCount = 0;
        int maxCount = 0;
        double exactSum = 0.0;

        for (int i = 0; i < _count; i++)
        {
            double val = _buffer[IndexAt(i)].Value;
            exactSum += val;

            if (val < newMin)
            {
                newMin = val;
                minCount = 1;
            }
            else if (val == newMin)
            {
                minCount++;
            }

            if (val > newMax)
            {
                newMax = val;
                maxCount = 1;
            }
            else if (val == newMax)
            {
                maxCount++;
            }
        }

        _min = newMin;
        _max = newMax;
        _minCount = minCount;
        _maxCount = maxCount;
        _sum = exactSum;
    }

    private void EnsureCapacityInternal()
    {
        if (_count == _buffer.Length)
        {
            int newCap = _buffer.Length * 2;
            var newBuf = new TimestampedReading[newCap];
            for (int i = 0; i < _count; i++)
            {
                newBuf[i] = _buffer[IndexAt(i)];
            }
            _buffer = newBuf;
            _head = 0;
        }
    }

    private void CompactBufferInternal()
    {
        int newCap = Math.Max(InitialCapacity, _count * 2);
        if (newCap < _buffer.Length)
        {
            var newBuf = new TimestampedReading[newCap];
            double exactSum = 0.0;
            for (int i = 0; i < _count; i++)
            {
                var reading = _buffer[IndexAt(i)];
                newBuf[i] = reading;
                exactSum += reading.Value;
            }
            _buffer = newBuf;
            _head = 0;
            _sum = exactSum;
        }
    }

    /// <summary>
    /// Gets a consistent snapshot of the active window aggregate for this device.
    /// Returns false if there are no active readings inside the active window.
    /// </summary>
    public bool TryGetAggregate(DateTimeOffset now, TimeSpan windowDuration, out DeviceAggregate aggregate)
    {
        lock (_gate)
        {
            if (_isRetired)
            {
                aggregate = default;
                return false;
            }

            long cutoffTicks = now.UtcTicks - windowDuration.Ticks;
            long nowTicks = now.UtcTicks;

            ExpireOldReadingsInternal(cutoffTicks);

            if (_count == 0)
            {
                aggregate = default;
                return false;
            }

            // Window boundary convention: (now - windowDuration, now]
            // In typical scenarios all buffered readings have Ticks <= nowTicks
            if (_buffer[IndexAt(_count - 1)].Ticks <= nowTicks)
            {
                aggregate = new DeviceAggregate(DeviceId, _count, _min, _max, _sum / _count);
                return true;
            }

            // Rare clock skew edge case: future timestamps are present at tail of buffer
            int activeCount = 0;
            double subsetMin = double.PositiveInfinity;
            double subsetMax = double.NegativeInfinity;
            double subsetSum = 0.0;

            for (int i = 0; i < _count; i++)
            {
                var item = _buffer[IndexAt(i)];
                if (item.Ticks > nowTicks)
                {
                    break;
                }

                activeCount++;
                subsetSum += item.Value;
                if (item.Value < subsetMin) subsetMin = item.Value;
                if (item.Value > subsetMax) subsetMax = item.Value;
            }

            if (activeCount == 0)
            {
                aggregate = default;
                return false;
            }

            aggregate = new DeviceAggregate(DeviceId, activeCount, subsetMin, subsetMax, subsetSum / activeCount);
            return true;
        }
    }

    /// <summary>
    /// Attempts to retire this device state if it has been idle and has no active readings.
    /// </summary>
    public bool TryRetireIfInactive(long cutoffTicks, long nowTicks, long inactiveThresholdTicks, out bool canRemove)
    {
        lock (_gate)
        {
            ExpireOldReadingsInternal(cutoffTicks);

            if (_count == 0 && (nowTicks - _lastIngestTicks >= inactiveThresholdTicks))
            {
                _isRetired = true;
                _buffer = Array.Empty<TimestampedReading>();
                _head = 0;
                _count = 0;
                _sum = 0.0;
                _min = double.PositiveInfinity;
                _max = double.NegativeInfinity;
                _minCount = 0;
                _maxCount = 0;
                canRemove = true;
                return true;
            }

            canRemove = false;
            return false;
        }
    }

    /// <summary>
    /// Internal count for testing and diagnostic verification.
    /// </summary>
    internal int ActiveCount
    {
        get
        {
            lock (_gate)
            {
                return _count;
            }
        }
    }
}
