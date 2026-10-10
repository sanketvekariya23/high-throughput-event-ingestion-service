namespace HighThroughputEventIngestion.Core.Exceptions;

/// <summary>
/// Exception thrown when an individual reading fails domain validation.
/// </summary>
public sealed class ReadingValidationException : Exception
{
    public ReadingValidationException(string message) : base(message)
    {
    }

    public ReadingValidationException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
