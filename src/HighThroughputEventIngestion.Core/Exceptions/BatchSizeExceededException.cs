namespace HighThroughputEventIngestion.Core.Exceptions;

/// <summary>
/// Exception thrown when an ingestion batch contains more readings than allowed by configuration.
/// </summary>
public sealed class BatchSizeExceededException : Exception
{
    public BatchSizeExceededException(string message) : base(message)
    {
    }

    public BatchSizeExceededException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
