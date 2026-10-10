using HighThroughputEventIngestion.Core.Model;

namespace HighThroughputEventIngestion.Core.Services;

/// <summary>
/// Service responsible for querying rolling active-window aggregates.
/// </summary>
public interface IAggregateService
{
    /// <summary>
    /// Attempts to retrieve the current active-window aggregate for a device.
    /// Returns false if the device does not exist or has no active readings inside the 5-minute window.
    /// </summary>
    bool TryGetAggregate(string deviceId, out DeviceAggregate aggregate);

    /// <summary>
    /// Prunes inactive devices that have no active readings and have exceeded the inactivity threshold.
    /// </summary>
    int PruneInactiveDevices();

    /// <summary>
    /// Returns the count of registered device instances.
    /// </summary>
    int DeviceCount { get; }
}
