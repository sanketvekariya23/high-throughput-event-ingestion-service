using System.Collections.Concurrent;

namespace HighThroughputEventIngestion.Core.Services.Storage;

/// <summary>
/// High-concurrency device registry implementation backed by ConcurrentDictionary.
/// Avoids global locks and supports fine-grained device retirement.
/// </summary>
internal sealed class ConcurrentDeviceStore : IDeviceStore
{
    private readonly ConcurrentDictionary<string, DeviceState> _devices = new(StringComparer.Ordinal);

    public DeviceState GetOrCreate(string deviceId)
    {
        return _devices.GetOrAdd(deviceId, static id => new DeviceState(id));
    }

    public bool TryGet(string deviceId, out DeviceState state)
    {
        return _devices.TryGetValue(deviceId, out state!);
    }

    public int DeviceCount => _devices.Count;

    public int PruneInactiveDevices(DateTimeOffset now, TimeSpan windowDuration, TimeSpan inactiveThreshold)
    {
        long nowTicks = now.UtcTicks;
        long cutoffTicks = nowTicks - windowDuration.Ticks;
        long inactiveTicks = inactiveThreshold.Ticks;
        int prunedCount = 0;

        foreach (var kvp in _devices)
        {
            var deviceState = kvp.Value;
            if (deviceState.TryRetireIfInactive(cutoffTicks, nowTicks, inactiveTicks, out bool canRemove))
            {
                if (canRemove && _devices.TryRemove(new KeyValuePair<string, DeviceState>(kvp.Key, deviceState)))
                {
                    prunedCount++;
                }
            }
        }

        return prunedCount;
    }

    public void Clear()
    {
        _devices.Clear();
    }
}
