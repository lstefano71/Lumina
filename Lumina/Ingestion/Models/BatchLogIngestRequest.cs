namespace Lumina.Ingestion.Models;

/// <summary>
/// Batch ingestion request for multiple log entries.
/// </summary>
public sealed class BatchLogIngestRequest
{
    /// <summary>
    /// Gets or sets the stream name for all entries in this batch.
    /// Can be overridden by individual entries.
    /// </summary>
    public required string Stream { get; init; }
    
    /// <summary>
    /// Gets or sets the log entries in this batch.
    /// </summary>
    public required IReadOnlyList<LogIngestRequest> Entries { get; init; }
}