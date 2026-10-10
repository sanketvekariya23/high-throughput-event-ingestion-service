using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services.Storage;

namespace HighThroughputEventIngestion.Core.Services;

/// <summary>
/// Service responsible for querying rolling active-window aggregates.
/// </summary>
internal sealed class AggregateService : IAggregateService
{
    private readonly IDeviceStore _deviceStore;
    private readonly TimeProvider _timeProvider;
    private readonly IngestionOptions _options;

    public AggregateService(
        IDeviceStore deviceStore,
        TimeProvider timeProvider,
        IngestionOptions? options = null)
    {
        _deviceStore = deviceStore ?? throw new ArgumentNullException(nameof(deviceStore));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _options = options ?? new IngestionOptions();
    }

    public bool TryGetAggregate(string deviceId, out DeviceAggregate aggregate)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            aggregate = default;
            return false;
        }

        if (_deviceStore.TryGet(deviceId, out var state))
        {
            DateTimeOffset now = _timeProvider.GetUtcNow();
            return state.TryGetAggregate(now, _options.WindowDuration, out aggregate);
        }

        aggregate = default;
        return false;
    }

    public int PruneInactiveDevices()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        return _deviceStore.PruneInactiveDevices(now, _options.WindowDuration, _options.InactiveDeviceThreshold);
    }

    public int DeviceCount => _deviceStore.DeviceCount;
}
