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
  /// Default is "data/storage/lumina.duckdb".
  /// </summary>
  public string DatabasePath { get; init; } = "data/storage/lumina.duckdb";

  /// <summary>
  /// Gets the interval in seconds between stream discovery refresh operations.
  /// Default is 60 seconds.
  /// </summary>
  public int RefreshStreamsIntervalSeconds { get; init; } = 60;

  /// <summary>
  /// Gets a value indicating whether external file access is allowed in queries.
  /// When enabled, users can reference external files via DuckDB's read functions.
  /// Default is true.
  /// </summary>
  public bool AllowExternalFileAccess { get; init; } = true;

  /// <summary>
  /// Gets the allowed protocols for external file access.
  /// Default is ["http", "https", "file", "s3"].
  /// Only used when AllowExternalFileAccess is true.
  /// </summary>
  public IReadOnlyList<string> AllowedExternalProtocols { get; init; } = new List<string> { "http", "https", "file", "s3" }.AsReadOnly();

  /// <summary>
  /// Gets a value indicating whether SQL validation is enabled.
  /// When enabled, queries are validated to ensure they are SELECT-only.
  /// Default is true.
  /// </summary>
  public bool EnableSqlValidation { get; init; } = true;

  /// <summary>
  /// Gets the refresh interval as a TimeSpan.
  /// </summary>
  public TimeSpan RefreshInterval => TimeSpan.FromSeconds(RefreshStreamsIntervalSeconds);
}
