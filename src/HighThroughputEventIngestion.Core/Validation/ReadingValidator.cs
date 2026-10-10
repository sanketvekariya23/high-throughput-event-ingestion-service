using HighThroughputEventIngestion.Core.Exceptions;
using HighThroughputEventIngestion.Core.Model;

namespace HighThroughputEventIngestion.Core.Validation;

/// <summary>
/// Provides high-performance domain validation for incoming device readings.
/// </summary>
public static class ReadingValidator
{
    /// <summary>
    /// Validates an individual reading against domain rules and clock constraints.
    /// </summary>
    public static void Validate(in Reading reading, TimeSpan futureTolerance, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(reading.DeviceId))
        {
            throw new ReadingValidationException("deviceId is required and cannot be empty or whitespace.");
        }

        if (reading.Timestamp == default || reading.Timestamp.Year < 1970 || reading.Timestamp.Year > 3000)
        {
            throw new ReadingValidationException("timestamp must be a valid ISO-8601 UTC timestamp.");
        }

        if (!double.IsFinite(reading.Value))
        {
            throw new ReadingValidationException($"value must be a finite number. Provided: {reading.Value}.");
        }

        if (reading.Timestamp > now + futureTolerance)
        {
            throw new ReadingValidationException(
                $"timestamp '{reading.Timestamp:O}' is too far in the future relative to server time '{now:O}' (allowed skew: {futureTolerance.TotalMinutes} min).");
        }
    }
}
