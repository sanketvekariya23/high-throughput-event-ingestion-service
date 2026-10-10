namespace HighThroughputEventIngestion.Core.Services.Storage;

/// <summary>
/// Compact 16-byte unmanaged value type representing a reading in device memory.
/// Avoids heap allocations and optimizes CPU cache locality.
/// </summary>
internal readonly record struct TimestampedReading(long Ticks, double Value);
