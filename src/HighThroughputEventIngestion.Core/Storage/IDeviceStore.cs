namespace HighThroughputEventIngestion.Core.Storage;

/// <summary>
/// Thread-safe registry and lifecycle manager for all device states.
/// </summary>
internal interface IDeviceStore
{
    /// <summary>
    /// Gets existing state for a device or creates a new one.
    /// </summary>
    DeviceState GetOrCreate(string deviceId);

    /// <summary>
    /// Attempts to retrieve existing state for a device.
    /// </summary>
    bool TryGet(string deviceId, out DeviceState state);

    /// <summary>
    /// Total count of registered device instances.
    /// </summary>
    int DeviceCount { get; }

    /// <summary>
    /// Scans registered devices and prunes those that have no active readings and have exceeded the inactivity threshold.
    /// Returns the number of devices pruned.
    /// </summary>
    int PruneInactiveDevices(DateTimeOffset now, TimeSpan windowDuration, TimeSpan inactiveThreshold);

    /// <summary>
    /// Clears all registered devices.
    /// </summary>
    void Clear();
}
