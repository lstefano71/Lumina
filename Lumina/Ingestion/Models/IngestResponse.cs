namespace Lumina.Ingestion.Models;

/// <summary>
/// Response for log ingestion requests.
/// </summary>
public sealed class IngestResponse
{
    /// <summary>
    /// Gets or sets a value indicating whether the ingestion was successful.
    /// </summary>
    public bool Success { get; init; }
    
    /// <summary>
    /// Gets or sets the number of entries accepted.
    /// </summary>
    public int EntriesAccepted { get; init; }
    
    /// <summary>
    /// Gets or sets the error message if ingestion failed.
    /// </summary>
    public string? Error { get; init; }
    
    /// <summary>
    /// Gets or sets the timestamp of ingestion.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    
    /// <summary>
    /// Creates a successful response.
    /// </summary>
    public static IngestResponse Ok(int entriesAccepted) => new()
    {
        Success = true,
        EntriesAccepted = entriesAccepted
    };
    
    /// <summary>
    /// Creates an error response.
    /// </summary>
    public static IngestResponse Fail(string error, int entriesAccepted = 0) => new()
    {
        Success = false,
        EntriesAccepted = entriesAccepted,
        Error = error
    };
}