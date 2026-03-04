using DuckDB.NET.Data;

using Lumina.Core.Configuration;
using Lumina.Storage.Wal;

using System.Text;
using System.Text.Json;

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
  public IReadOnlyList<string> RegisteredStreams { get; init; } = Array.Empty<string>();
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
  private readonly HashSet<string> _registeredStreams = new(StringComparer.OrdinalIgnoreCase);
  private readonly HashSet<string> _hotTableStreams = new(StringComparer.OrdinalIgnoreCase);
  private readonly object _registrationLock = new();
  private DateTime _lastRefreshTime = DateTime.MinValue;

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
  /// Gets the list of currently registered stream names.
  /// </summary>
  public IReadOnlyList<string> GetRegisteredStreams()
  {
    lock (_registrationLock) {
      return _registeredStreams.OrderBy(s => s).ToList();
    }
  }

  /// <summary>
  /// Gets the last refresh time for stream registrations.
  /// </summary>
  public DateTime LastRefreshTime => _lastRefreshTime;

  /// <summary>
  /// Initializes the DuckDB connection and registers all streams.
  /// </summary>
  public async Task InitializeAsync(CancellationToken cancellationToken = default)
  {
    _connection = new DuckDBConnection("DataSource=:memory:");
    await _connection.OpenAsync(cancellationToken);

    _logger.LogInformation("DuckDB in-memory connection opened");

    // Register all discovered streams as views
    await RegisterStreamTablesAsync(cancellationToken);
  }

  /// <summary>
  /// Discovers and registers all streams as DuckDB views.
  /// </summary>
  public async Task RegisterStreamTablesAsync(CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (_connection == null) {
      await InitializeAsync(cancellationToken);
      return;
    }

    var mappings = _parquetManager.GetStreamMappings();
    var registered = new List<string>();

    foreach (var mapping in mappings) {
      cancellationToken.ThrowIfCancellationRequested();

      try {
        var createViewSql = mapping.GetCreateViewSql();
        await ExecuteNonQueryAsync(createViewSql, cancellationToken);

        lock (_registrationLock) {
          _registeredStreams.Add(mapping.StreamName);
        }

        registered.Add(mapping.StreamName);
        _logger.LogDebug("Registered stream '{Stream}' as view with {FileCount} files",
            mapping.StreamName, mapping.ParquetFiles.Count);
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to register stream '{Stream}' as view", mapping.StreamName);
      }
    }

    _lastRefreshTime = DateTime.UtcNow;
    _logger.LogInformation("Registered {Count} streams as DuckDB views", registered.Count);
  }

  /// <summary>
  /// Refreshes stream registrations (adds new streams, updates existing).
  /// </summary>
  public async Task RefreshStreamsAsync(CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    _logger.LogDebug("Refreshing stream registrations...");

    await RegisterStreamTablesAsync(cancellationToken);
  }

  /// <summary>
  /// Registers a single stream as a DuckDB view.
  /// </summary>
  /// <param name="streamName">The stream name to register.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task<bool> RegisterStreamAsync(string streamName, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    var mapping = _parquetManager.GetStreamMapping(streamName);
    if (mapping == null) {
      return false;
    }

    try {
      // Drop existing view if any
      var dropViewSql = mapping.GetDropViewSql();
      await ExecuteNonQueryAsync(dropViewSql, cancellationToken);

      // Create new view
      var createViewSql = mapping.GetCreateViewSql();
      await ExecuteNonQueryAsync(createViewSql, cancellationToken);

      lock (_registrationLock) {
        _registeredStreams.Add(streamName);
      }

      _logger.LogDebug("Registered stream '{Stream}' as view", streamName);
      return true;
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Failed to register stream '{Stream}' as view", streamName);
      return false;
    }
  }

  /// <summary>
  /// Executes a SQL query over Parquet files.
  /// </summary>
  /// <param name="sql">The SQL query.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result.</returns>
  public async Task<QueryResult> ExecuteQueryAsync(string sql, CancellationToken cancellationToken = default)
  {
    return await ExecuteQueryAsync(sql, null, cancellationToken);
  }

  /// <summary>
  /// Executes a SQL query with optional parameters.
  /// </summary>
  /// <param name="sql">The SQL query.</param>
  /// <param name="parameters">Optional query parameters.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The query result.</returns>
  public async Task<QueryResult> ExecuteQueryAsync(
      string sql,
      Dictionary<string, object?>? parameters,
      CancellationToken cancellationToken = default)
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

      // Add parameters if provided
      if (parameters != null) {
        foreach (var (name, value) in parameters) {
          var paramName = name.StartsWith("$") ? name : $"${name}";
          var duckDbParam = new DuckDBParameter(paramName, value ?? DBNull.Value);
          command.Parameters.Add(duckDbParam);
        }
      }

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

    IReadOnlyList<string> registeredStreams;
    lock (_registrationLock) {
      registeredStreams = _registeredStreams.OrderBy(s => s).ToList();
    }

    return new QueryResult {
      Rows = rows,
      RowCount = rows.Count,
      Columns = columns,
      ExecutionTime = stopwatch.Elapsed,
      RegisteredStreams = registeredStreams
    };
  }

  /// <summary>
  /// Executes a non-query SQL statement (e.g., CREATE VIEW).
  /// </summary>
  /// <param name="sql">The SQL statement.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (_connection == null) {
      await InitializeAsync(cancellationToken);
    }

    try {
      using var command = _connection!.CreateCommand();
      command.CommandText = sql;
      await command.ExecuteNonQueryAsync(cancellationToken);
    } catch (Exception ex) {
      _logger.LogError(ex, "Non-query execution failed: {SQL}", sql);
      throw;
    }
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
      conditions.Add($"_t >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (end.HasValue) {
      conditions.Add($"_t <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (!string.IsNullOrEmpty(level)) {
      conditions.Add($"level = '{level.ToLower()}'");
    }

    var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";

    var sql = $@"
            SELECT * FROM read_parquet([{fileList}],union_by_name=true)
            {whereClause}
            ORDER BY _t DESC
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
            SELECT * FROM read_parquet([{fileList}],union_by_name=true)
            WHERE message ILIKE '%{searchTerm}%'
               OR stream ILIKE '%{searchTerm}%'
            ORDER BY _t DESC
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
      conditions.Add($"_t >= '{start.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    if (end.HasValue) {
      conditions.Add($"_t <= '{end.Value:yyyy-MM-dd HH:mm:ss}'");
    }

    var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";

    var sql = $@"
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT level) as level_count,
                MIN(_t) as earliest_timestamp,
                MAX(_t) as latest_timestamp,
                level,
                COUNT(*) as count_by_level
            FROM read_parquet([{fileList}],union_by_name=true)
            {whereClause}
            GROUP BY level
            ORDER BY count_by_level DESC";

    return await ExecuteQueryAsync(sql, cancellationToken);
  }

  /// <summary>
  /// Materializes hot buffer entries into a DuckDB in-memory table for the given stream.
  /// Called by LiveQueryRefreshService on each tick when the buffer version has changed.
  /// </summary>
  public async Task RefreshHotBufferAsync(string stream, IReadOnlyList<BufferedEntry> entries, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (_connection == null) return;

    var tableName = GetHotTableName(stream);

    // Ensure the hot table exists
    await EnsureHotTableAsync(stream, cancellationToken);

    // Clear existing data
    await ExecuteNonQueryAsync($"DELETE FROM {tableName}", cancellationToken);

    if (entries.Count == 0) {
      // Still rebuild the view so the stream is queryable (returns 0 rows)
      await RebuildStreamViewAsync(stream, cancellationToken);
      return;
    }

    // Insert in batches to avoid SQL statement size limits
    const int batchSize = 500;
    for (int i = 0; i < entries.Count; i += batchSize) {
      var batch = entries.Skip(i).Take(batchSize).ToList();
      var sb = new StringBuilder();
      sb.Append($"INSERT INTO {tableName} (stream, _t, level, message, trace_id, span_id, duration_ms, _meta) VALUES ");

      for (int j = 0; j < batch.Count; j++) {
        if (j > 0) sb.Append(", ");
        var e = batch[j].LogEntry;
        var meta = e.Attributes.Count > 0
            ? EscapeSqlString(JsonSerializer.Serialize(e.Attributes))
            : null;

        sb.Append('(');
        sb.Append($"'{EscapeSqlString(e.Stream)}'");
        sb.Append(", ");
        sb.Append($"'{e.Timestamp:yyyy-MM-dd HH:mm:ss.fffffff}'::TIMESTAMP");
        sb.Append(", ");
        sb.Append($"'{EscapeSqlString(e.Level)}'");
        sb.Append(", ");
        sb.Append($"'{EscapeSqlString(e.Message)}'");
        sb.Append(", ");
        sb.Append(e.TraceId != null ? $"'{EscapeSqlString(e.TraceId)}'" : "NULL");
        sb.Append(", ");
        sb.Append(e.SpanId != null ? $"'{EscapeSqlString(e.SpanId)}'" : "NULL");
        sb.Append(", ");
        sb.Append(e.DurationMs.HasValue ? e.DurationMs.Value.ToString() : "NULL");
        sb.Append(", ");
        sb.Append(meta != null ? $"'{meta}'" : "NULL");
        sb.Append(')');
      }

      await ExecuteNonQueryAsync(sb.ToString(), cancellationToken);
    }

    // Now rebuild the view to include the hot table
    await RebuildStreamViewAsync(stream, cancellationToken);

    _logger.LogDebug("Refreshed hot buffer for stream '{Stream}' with {Count} entries", stream, entries.Count);
  }

  /// <summary>
  /// Clears the hot table for a stream (called after compaction).
  /// </summary>
  public async Task ClearHotTableAsync(string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (_connection == null) return;

    var tableName = GetHotTableName(stream);
    lock (_registrationLock) {
      if (!_hotTableStreams.Contains(stream)) return;
    }

    await ExecuteNonQueryAsync($"DELETE FROM {tableName}", cancellationToken);
    _logger.LogDebug("Cleared hot table for stream '{Stream}'", stream);
  }

  /// <summary>
  /// Ensures the hot in-memory table exists for a stream.
  /// </summary>
  public async Task EnsureHotTableAsync(string stream, CancellationToken cancellationToken = default)
  {
    lock (_registrationLock) {
      if (_hotTableStreams.Contains(stream)) return;
    }

    var tableName = GetHotTableName(stream);
    var createSql = $@"CREATE TABLE IF NOT EXISTS {tableName} (
      stream VARCHAR,
      _t TIMESTAMP,
      level VARCHAR,
      message VARCHAR,
      trace_id VARCHAR,
      span_id VARCHAR,
      duration_ms INTEGER,
      _meta VARCHAR
    )";

    await ExecuteNonQueryAsync(createSql, cancellationToken);

    lock (_registrationLock) {
      _hotTableStreams.Add(stream);
    }
  }

  /// <summary>
  /// Rebuilds the view for a stream to include both Parquet files and the hot table.
  /// </summary>
  public async Task RebuildStreamViewAsync(string stream, CancellationToken cancellationToken = default)
  {
    var mapping = _parquetManager.GetStreamMapping(stream);
    var hotTableName = GetHotTableName(stream);

    bool hasHotTable;
    lock (_registrationLock) {
      hasHotTable = _hotTableStreams.Contains(stream);
    }

    string viewSql;
    if (mapping != null && mapping.ParquetFiles.Count > 0) {
      var parquetSql = mapping.GetParquetReadSql();
      viewSql = hasHotTable
          ? $"CREATE OR REPLACE VIEW {mapping.GetViewName()} AS SELECT * FROM ({parquetSql} UNION ALL SELECT * FROM {hotTableName})"
          : mapping.GetCreateViewSql();
    } else {
      // No Parquet files yet — view reads only from the hot table
      var viewName = StreamTableMapping.EscapeIdentifierPublic(stream);
      viewSql = hasHotTable
          ? $"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM {hotTableName}"
          : $"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM (SELECT NULL LIMIT 0)";
    }

    await ExecuteNonQueryAsync(viewSql, cancellationToken);

    lock (_registrationLock) {
      _registeredStreams.Add(stream);
    }
  }

  /// <summary>
  /// Gets the DuckDB table name for the hot buffer of a stream.
  /// </summary>
  public static string GetHotTableName(string stream)
  {
    // Sanitize for SQL identifier usage
    var sanitized = stream.Replace("-", "_").Replace(".", "_").Replace(" ", "_");
    return $"_hot_{sanitized}";
  }

  private static string EscapeSqlString(string s) => s.Replace("'", "''");

  public void Dispose()
  {
    if (_disposed) {
      return;
    }

    _disposed = true;
    _connection?.Dispose();
  }
}