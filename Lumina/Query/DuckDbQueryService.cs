using DuckDB.NET.Data;

using Lumina.Core.Configuration;

namespace Lumina.Query;

/// <summary>
/// Query result containing rows and metadata.
/// </summary>
public sealed class QueryResult
{
  public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; init; } = Array.Empty<IReadOnlyDictionary<string, object?>>();
  public int RowCount { get; init; }
  public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
  public TimeSpan ExecutionTime { get; init; }
}

/// <summary>
/// DuckDB-based query service for SQL queries over Parquet files.
/// </summary>
public sealed class DuckDbQueryService : IDisposable
{
  private readonly QuerySettings _settings;
  private readonly ParquetManager _parquetManager;
  private readonly ILogger<DuckDbQueryService> _logger;
  private DuckDBConnection? _connection;
  private bool _disposed;

  public DuckDbQueryService(
      QuerySettings settings,
      ParquetManager parquetManager,
      ILogger<DuckDbQueryService> logger)
  {
    _settings = settings;
    _parquetManager = parquetManager;
    _logger = logger;
  }

  /// <summary>
  /// Initializes the DuckDB connection.
  /// </summary>
  public async Task InitializeAsync(CancellationToken cancellationToken = default)
  {
    var connectionString = $"Data Source={_settings.DatabasePath}";
    _connection = new DuckDBConnection(connectionString);
    await _connection.OpenAsync(cancellationToken);

    _logger.LogInformation("DuckDB connection opened at {Path}", _settings.DatabasePath);
  }

  /// <summary>
  /// Executes a SQL query over Parquet files.
  /// </summary>
  /// <param name="sql">The SQL query.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result.</returns>
  public async Task<QueryResult> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (_connection == null) {
      await InitializeAsync(cancellationToken);
    }

    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    var rows = new List<IReadOnlyDictionary<string, object?>>();
    var columns = new List<string>();

    try {
      using var command = _connection!.CreateCommand();
      command.CommandText = sql;

      using var reader = await command.ExecuteReaderAsync(cancellationToken);

      // Get column names
      for (int i = 0; i < reader.FieldCount; i++) {
        columns.Add(reader.GetName(i));
      }

      // Read rows
      while (await reader.ReadAsync(cancellationToken)) {
        var row = new Dictionary<string, object?>();

        for (int i = 0; i < reader.FieldCount; i++) {
          var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
          row[columns[i]] = value;
        }

        rows.Add(row);
      }
    } catch (Exception ex) {
      _logger.LogError(ex, "Query execution failed: {SQL}", sql);
      throw;
    }

    stopwatch.Stop();

    return new QueryResult {
      Rows = rows,
      RowCount = rows.Count,
      Columns = columns,
      ExecutionTime = stopwatch.Elapsed
    };
  }

  /// <summary>
  /// Queries logs from a specific stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="start">Start time.</param>
  /// <param name="end">End time.</param>
  /// <param name="level">Optional log level filter.</param>
  /// <param name="limit">Maximum number of results.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result.</returns>
  public async Task<QueryResult> QueryLogsAsync(
      string stream,
      DateTime? start = null,
      DateTime? end = null,
      string? level = null,
      int limit = 1000,
      CancellationToken cancellationToken = default)
  {
    var files = _parquetManager.GetStreamFiles(stream);

    if (files.Count == 0) {
      return new QueryResult {
        Rows = Array.Empty<IReadOnlyDictionary<string, object?>>(),
        RowCount = 0,
        Columns = Array.Empty<string>(),
        ExecutionTime = TimeSpan.Zero
      };
    }

    // Build SQL query using read_parquet
    var fileList = string.Join(", ", files.Select(f => $"'{f}'"));
    var conditions = new List<string>();

    if (start.HasValue) {
      conditions.Add($"timestamp >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (end.HasValue) {
      conditions.Add($"timestamp <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (!string.IsNullOrEmpty(level)) {
      conditions.Add($"level = '{level.ToLower()}'");
    }

    var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";

    var sql = $@"
            SELECT * FROM read_parquet([{fileList}])
            {whereClause}
            ORDER BY timestamp DESC
            LIMIT {limit}";

    return await ExecuteQueryAsync(sql, cancellationToken);
  }

  /// <summary>
  /// Queries logs with a full-text search.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="searchTerm">The search term.</param>
  /// <param name="limit">Maximum number of results.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result.</returns>
  public async Task<QueryResult> SearchLogsAsync(
      string stream,
      string searchTerm,
      int limit = 1000,
      CancellationToken cancellationToken = default)
  {
    var files = _parquetManager.GetStreamFiles(stream);

    if (files.Count == 0) {
      return new QueryResult {
        Rows = Array.Empty<IReadOnlyDictionary<string, object?>>(),
        RowCount = 0,
        Columns = Array.Empty<string>(),
        ExecutionTime = TimeSpan.Zero
      };
    }

    var fileList = string.Join(", ", files.Select(f => $"'{f}'"));

    var sql = $@"
            SELECT * FROM read_parquet([{fileList}])
            WHERE message ILIKE '%{searchTerm}%'
               OR stream ILIKE '%{searchTerm}%'
            ORDER BY timestamp DESC
            LIMIT {limit}";

    return await ExecuteQueryAsync(sql, cancellationToken);
  }

  /// <summary>
  /// Gets aggregate statistics for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="start">Start time.</param>
  /// <param name="end">End time.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result with statistics.</returns>
  public async Task<QueryResult> GetStatsAsync(
      string stream,
      DateTime? start = null,
      DateTime? end = null,
      CancellationToken cancellationToken = default)
  {
    var files = _parquetManager.GetStreamFiles(stream);

    if (files.Count == 0) {
      return new QueryResult {
        Rows = Array.Empty<IReadOnlyDictionary<string, object?>>(),
        RowCount = 0,
        Columns = Array.Empty<string>(),
        ExecutionTime = TimeSpan.Zero
      };
    }

    var fileList = string.Join(", ", files.Select(f => $"'{f}'"));
    var conditions = new List<string>();

    if (start.HasValue) {
      conditions.Add($"timestamp >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (end.HasValue) {
      conditions.Add($"timestamp <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";

    var sql = $@"
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT level) as level_count,
                MIN(timestamp) as earliest_timestamp,
                MAX(timestamp) as latest_timestamp,
                level,
                COUNT(*) as count_by_level
            FROM read_parquet([{fileList}])
            {whereClause}
            GROUP BY level
            ORDER BY count_by_level DESC";

    return await ExecuteQueryAsync(sql, cancellationToken);
  }

  public void Dispose()
  {
    if (_disposed) {
      return;
    }

    _disposed = true;
    _connection?.Dispose();
  }
}