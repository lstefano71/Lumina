namespace Lumina.Core.Configuration;

/// <summary>
/// Configuration settings for the ingestion layer.
/// </summary>
public sealed class IngestionSettings
{
    /// <summary>
    /// Gets the HTTP port for the ingestion server.
    /// Default is 5000.
    /// </summary>
    public int HttpPort { get; init; } = 5000;
    
    /// <summary>
    /// Gets the maximum request body size in bytes.
    /// Default is 10 MB.
    /// </summary>
    public int MaxRequestBodySize { get; init; } = 10 * 1024 * 1024;
}