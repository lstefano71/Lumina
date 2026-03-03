namespace Lumina.Core.Configuration;

/// <summary>
/// Main application configuration for Lumina.
/// </summary>
public sealed class LuminaSettings
{
    /// <summary>
    /// Gets the Write-Ahead Log storage settings.
    /// </summary>
    public WalSettings Wal { get; init; } = new();
    
    /// <summary>
    /// Gets the compaction settings.
    /// </summary>
    public CompactionSettings Compaction { get; init; } = new();
    
    /// <summary>
    /// Gets the query layer settings.
    /// </summary>
    public QuerySettings Query { get; init; } = new();
    
    /// <summary>
    /// Gets the ingestion layer settings.
    /// </summary>
    public IngestionSettings Ingestion { get; init; } = new();
    
    /// <summary>
    /// Gets the deployment mode.
    /// </summary>
    public DeploymentMode DeploymentMode { get; init; } = DeploymentMode.Intranet;
}