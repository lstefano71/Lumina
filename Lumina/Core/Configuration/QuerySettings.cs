namespace Lumina.Core.Configuration;

/// <summary>
/// Configuration settings for the query layer.
/// </summary>
public sealed class QuerySettings
{
    /// <summary>
    /// Gets the maximum number of results returned by a query.
    /// Default is 10000.
    /// </summary>
    public int MaxResults { get; init; } = 10000;
    
    /// <summary>
    /// Gets the timeout for query execution.
    /// Default is 30 seconds.
    /// </summary>
    public TimeSpan QueryTimeout { get; init; } = TimeSpan.FromSeconds(30);
    
    /// <summary>
    /// Gets the path to the DuckDB database file.
    /// Default is "data/lumina.duckdb".
    /// </summary>
    public string DatabasePath { get; init; } = "data/lumina.duckdb";
}
