namespace Lumina.Ingestion.Models;

/// <summary>
/// JSON ingestion request for a single log entry.
/// </summary>
public sealed class LogIngestRequest
{
    /// <summary>
    /// Gets or sets the stream name for this log entry.
    /// </summary>
    public required string Stream { get; init; }
    
    /// <summary>
    /// Gets or sets the timestamp when the log entry was created.
    /// Defaults to UTC now if not specified.
    /// </summary>
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    
    /// <summary>
    /// Gets or sets the log level (e.g., "debug", "info", "warn", "error", "fatal").
    /// </summary>
    public required string Level { get; init; }
    
    /// <summary>
    /// Gets or sets the log message content.
    /// </summary>
    public required string Message { get; init; }
    
    /// <summary>
    /// Gets or sets the attribute dictionary containing additional structured data.
    /// </summary>
    public Dictionary<string, object?>? Attributes { get; init; }
    
    /// <summary>
    /// Gets or sets the trace ID for distributed tracing correlation.
    /// </summary>
    public string? TraceId { get; init; }
    
    /// <summary>
    /// Gets or sets the span ID for distributed tracing correlation.
    /// </summary>
    public string? SpanId { get; init; }
    
    /// <summary>
    /// Gets or sets the duration in milliseconds for timed operations.
    /// </summary>
    public int? DurationMs { get; init; }
}