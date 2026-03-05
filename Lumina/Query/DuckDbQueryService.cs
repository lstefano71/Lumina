using DuckDB.NET.Data;

using Lumina.Core.Concurrency;
using Lumina.Core.Configuration;
using Lumina.Storage.Parquet;
using Lumina.Storage.Wal;

using System.Collections.Concurrent;
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
  private readonly StreamLockManager _streamLockManager;
  private readonly SemaphoreSlim _dbLock = new(1, 1);
  private DuckDBConnection? _connection;
  private bool _disposed;
  private readonly HashSet<string> _registeredStreams = new(StringComparer.OrdinalIgnoreCase);
  private readonly ConcurrentDictionary<string, byte> _hotTableStreams = new(StringComparer.OrdinalIgnoreCase);
  private readonly ConcurrentDictionary<string, IReadOnlyList<ColumnSchema>> _hotTableSchemas = new(StringComparer.OrdinalIgnoreCase);
  private readonly object _registrationLock = new();
  private DateTime _lastRefreshTime = DateTime.MinValue;

  public DuckDbQueryService(
      QuerySettings settings,
      ParquetManager parquetManager,
      ILogger<DuckDbQueryService> logger,
      StreamLockManager? streamLockManager = null)
  {
    _settings = settings;
    _parquetManager = parquetManager;
    _logger = logger;
    _streamLockManager = streamLockManager ?? new StreamLockManager();
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
        // Use RebuildStreamViewAsync so the hot table (if any) is always
        // included in the view. Previously this called GetCreateViewSql()
        // which produced a parquet-only view, silently dropping hot-buffer
        // rows and causing query row-counts to jump up and down.
        await RebuildStreamViewAsync(mapping.StreamName, cancellationToken);

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
      // Use RebuildStreamViewAsync to preserve the hot table in the view.
      await RebuildStreamViewAsync(streamName, cancellationToken);

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
  /// Holds a <see cref="StreamLockManager.CompactionLock"/> reader lock for the
  /// duration of the query so that compaction cannot delete Parquet files mid-read.
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

    // Acquire a reader lock so compaction cannot delete Parquet files while
    // DuckDB is reading them.
    await using var guard = await _streamLockManager.CompactionLock
        .ReaderLockAsync(cancellationToken).ConfigureAwait(false);

    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    var rows = new List<IReadOnlyDictionary<string, object?>>();
    var columns = new List<string>();

    try {
      await _dbLock.WaitAsync(cancellationToken);
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
      } finally {
        _dbLock.Release();
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

    await _dbLock.WaitAsync(cancellationToken);
    try {
      await ExecuteNonQueryNoLockAsync(sql, cancellationToken);
    } catch (Exception ex) {
      _logger.LogError(ex, "Non-query execution failed: {SQL}", sql);
      throw;
    } finally {
      _dbLock.Release();
    }
  }

  private async Task ExecuteNonQueryNoLockAsync(string sql, CancellationToken cancellationToken = default)
  {
    using var command = _connection!.CreateCommand();
    command.CommandText = sql;
    await command.ExecuteNonQueryAsync(cancellationToken);
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
    // Reader lock covers file-path resolution + query execution so that
    // compaction cannot delete files between the two steps.
    await using var guard = await _streamLockManager.CompactionLock
        .ReaderLockAsync(cancellationToken).ConfigureAwait(false);

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
      conditions.Add($"_l = '{level.ToLower()}'");
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
    await using var guard = await _streamLockManager.CompactionLock
        .ReaderLockAsync(cancellationToken).ConfigureAwait(false);

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
            WHERE _m ILIKE '%{searchTerm}%'
               OR _s ILIKE '%{searchTerm}%'
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
    await using var guard = await _streamLockManager.CompactionLock
        .ReaderLockAsync(cancellationToken).ConfigureAwait(false);

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
                COUNT(DISTINCT _l) as level_count,
                MIN(_t) as earliest_timestamp,
                MAX(_t) as latest_timestamp,
                _l,
                COUNT(*) as count_by_level
            FROM read_parquet([{fileList}],union_by_name=true)
            {whereClause}
            GROUP BY _l
            ORDER BY count_by_level DESC";

    return await ExecuteQueryAsync(sql, cancellationToken);
  }

  /// <summary>
  /// Materializes hot buffer entries into a DuckDB in-memory table for the given stream.
  /// The table schema is resolved dynamically from the current entries (same logic as Parquet
  /// writing) so that promoted attribute columns — e.g. "version" — are queryable as
  /// top-level columns rather than being buried inside the _meta JSON blob.
  /// Called by LiveQueryRefreshService on each tick when the buffer version has changed.
  /// </summary>
  public async Task RefreshHotBufferAsync(string stream, IReadOnlyList<BufferedEntry> entries, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (_connection == null) return;

    var tableName = GetHotTableName(stream);

    // Resolve schema from current entries so that attribute columns promoted to top-level
    // in Parquet are also available in the hot table.
    var logEntries = entries.Select(e => e.LogEntry).ToList();
    var schema = logEntries.Count > 0
        ? SchemaResolver.ResolveSchema(logEntries)
        : GetFixedHotSchema();

    // Recreate the hot table when the column set has changed (or on first use).
    bool needsRecreate;
    lock (_registrationLock) {
      if (_hotTableSchemas.TryGetValue(stream, out var existingSchema)) {
        var existingCols = existingSchema.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var newCols = schema.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        needsRecreate = !existingCols.SetEquals(newCols);
      } else {
        needsRecreate = true;
      }
    }

    // Apply full refresh under one DB critical section so queries never observe
    // a transient empty/partial hot table between DELETE and INSERT batches.
    await _dbLock.WaitAsync(cancellationToken);
    try {
      if (needsRecreate) {
        await RecreateHotTableNoLockAsync(stream, tableName, schema, cancellationToken);
      } else {
        await ExecuteNonQueryNoLockAsync($"DELETE FROM {tableName}", cancellationToken);
      }

      if (entries.Count == 0) {
        // Still rebuild the view so the stream is queryable (returns 0 rows)
        await RebuildStreamViewNoLockAsync(stream, cancellationToken);
        return;
      }

      // Separate dynamic columns (promoted attributes) from the overflow _meta column.
      var dynamicCols = schema.Where(c => !IsFixedBaseColumn(c.Name) && !c.IsOverflow).ToList();
      var dynamicColNames = dynamicCols.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
      var hasOverflow = schema.Any(c => c.IsOverflow);

      // Build the INSERT column list.
      var colParts = new List<string> { "_s", "_t", "_l", "_m", "_traceid", "_spanid", "_duration_ms" };
      foreach (var dc in dynamicCols) colParts.Add($"\"{dc.Name}\"");
      if (hasOverflow) colParts.Add("_meta");
      var colList = string.Join(", ", colParts);

      // Insert in batches to avoid SQL statement size limits.
      const int batchSize = 500;
      for (int i = 0; i < entries.Count; i += batchSize) {
        var batch = entries.Skip(i).Take(batchSize).ToList();
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {tableName} ({colList}) VALUES ");

        for (int j = 0; j < batch.Count; j++) {
          if (j > 0) sb.Append(", ");
          var e = batch[j].LogEntry;

          // Attributes not promoted to a top-level column go into overflow.
          Dictionary<string, object?>? overflowAttrs = null;
          if (hasOverflow) {
            overflowAttrs = e.Attributes
                .Where(a => !dynamicColNames.Contains(a.Key))
                .ToDictionary(a => a.Key, a => a.Value);
          }

          sb.Append('(');
          sb.Append($"'{EscapeSqlString(e.Stream)}'");
          sb.Append(", ");
          sb.Append($"'{e.Timestamp:yyyy-MM-dd HH:mm:ss.fffffff}'::TIMESTAMP");
          sb.Append(", ");
          sb.Append(e.Level != null ? $"'{EscapeSqlString(e.Level)}'" : "NULL");
          sb.Append(", ");
          sb.Append($"'{EscapeSqlString(e.Message)}'");
          sb.Append(", ");
          sb.Append(e.TraceId != null ? $"'{EscapeSqlString(e.TraceId)}'" : "NULL");
          sb.Append(", ");
          sb.Append(e.SpanId != null ? $"'{EscapeSqlString(e.SpanId)}'" : "NULL");
          sb.Append(", ");
          sb.Append(e.DurationMs.HasValue ? e.DurationMs.Value.ToString() : "NULL");

          // Dynamic attribute columns.
          foreach (var dc in dynamicCols) {
            sb.Append(", ");
            sb.Append(e.Attributes.TryGetValue(dc.Name, out var val)
                ? FormatSqlValue(val, dc.Type)
                : "NULL");
          }

          // Overflow _meta.
          if (hasOverflow) {
            sb.Append(", ");
            sb.Append(overflowAttrs is { Count: > 0 }
                ? $"'{EscapeSqlString(JsonSerializer.Serialize(overflowAttrs))}'"
                : "NULL");
          }

          sb.Append(')');
        }

        await ExecuteNonQueryNoLockAsync(sb.ToString(), cancellationToken);
      }

      // Rebuild the view to reflect the updated hot table.
      await RebuildStreamViewNoLockAsync(stream, cancellationToken);
    } finally {
      _dbLock.Release();
    }

    _logger.LogDebug("Refreshed hot buffer for stream '{Stream}' with {Count} entries", stream, entries.Count);
  }

  // ---------------------------------------------------------------------------
  // Hot-table schema helpers
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Drops and recreates the hot table with a freshly resolved schema.
  /// Updates <see cref="_hotTableSchemas"/> and <see cref="_hotTableStreams"/>.
  /// </summary>
  private async Task RecreateHotTableAsync(string stream, string tableName, IReadOnlyList<ColumnSchema> schema, CancellationToken cancellationToken)
  {
    await _dbLock.WaitAsync(cancellationToken);
    try {
      await RecreateHotTableNoLockAsync(stream, tableName, schema, cancellationToken);
    } finally {
      _dbLock.Release();
    }
  }

  private async Task RecreateHotTableNoLockAsync(string stream, string tableName, IReadOnlyList<ColumnSchema> schema, CancellationToken cancellationToken)
  {
    await ExecuteNonQueryNoLockAsync($"DROP TABLE IF EXISTS {tableName}", cancellationToken);

    var colDefs = schema.Select(c => {
      var sqlType = SchemaTypeToSql(c.Type);
      var nullable = c.IsNullable ? "" : " NOT NULL";
      return $"\"{c.Name}\" {sqlType}{nullable}";
    });
    await ExecuteNonQueryNoLockAsync($"CREATE TABLE {tableName} ({string.Join(", ", colDefs)})", cancellationToken);

    lock (_registrationLock) {
      _hotTableStreams.TryAdd(stream, 0);
      _hotTableSchemas[stream] = schema;
    }
  }

  /// <summary>
  /// Returns the minimal fixed schema used when there are no entries to inspect.
  /// </summary>
  private static IReadOnlyList<ColumnSchema> GetFixedHotSchema() =>
  [
    new ColumnSchema { Name = "_s",          Type = SchemaType.String,    IsNullable = false },
    new ColumnSchema { Name = "_t",          Type = SchemaType.Timestamp, IsNullable = false },
    new ColumnSchema { Name = "_l",          Type = SchemaType.String,    IsNullable = true  },
    new ColumnSchema { Name = "_m",          Type = SchemaType.String,    IsNullable = false },
    new ColumnSchema { Name = "_traceid",    Type = SchemaType.String,    IsNullable = true  },
    new ColumnSchema { Name = "_spanid",     Type = SchemaType.String,    IsNullable = true  },
    new ColumnSchema { Name = "_duration_ms", Type = SchemaType.Int32,    IsNullable = true  },
  ];

  /// <summary>Maps a <see cref="SchemaType"/> to a DuckDB SQL type name.</summary>
  private static string SchemaTypeToSql(SchemaType type) => type switch {
    SchemaType.Boolean => "BOOLEAN",
    SchemaType.Int32 => "INTEGER",
    SchemaType.Int64 => "BIGINT",
    SchemaType.Float => "FLOAT",
    SchemaType.Double => "DOUBLE",
    SchemaType.Timestamp => "TIMESTAMP",
    SchemaType.Binary => "BLOB",
    _ => "VARCHAR"   // String, Json, Null
  };

  /// <summary>Formats a CLR value as a DuckDB SQL literal.</summary>
  private static string FormatSqlValue(object? value, SchemaType type) => value switch {
    null => "NULL",
    bool b => b ? "TRUE" : "FALSE",
    int i => i.ToString(),
    long l => l.ToString(),
    float f => f.ToString("G", System.Globalization.CultureInfo.InvariantCulture),
    double d => d.ToString("G", System.Globalization.CultureInfo.InvariantCulture),
    DateTime dt => $"'{dt.ToUniversalTime():yyyy-MM-dd HH:mm:ss.fffffff}'::TIMESTAMP",
    DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss.fffffff}'::TIMESTAMP",
    string s => $"'{EscapeSqlString(s)}'",
    _ => $"'{EscapeSqlString(value.ToString() ?? string.Empty)}'"
  };

  /// <summary>Returns true for column names that are always present in the fixed hot-table base.</summary>
  private static bool IsFixedBaseColumn(string name) =>
    name is "_s" or "_t" or "_l" or "_m" or "_traceid" or "_spanid" or "_duration_ms";

  /// <summary>
  /// Clears the hot table for a stream (called after compaction).
  /// </summary>
  public async Task ClearHotTableAsync(string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    if (_connection == null) return;

    var tableName = GetHotTableName(stream);
    lock (_registrationLock) {
      if (!_hotTableStreams.ContainsKey(stream)) return;
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
      if (_hotTableStreams.ContainsKey(stream)) return;
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
      _hotTableStreams.TryAdd(stream, 0);
    }
  }

  /// <summary>
  /// Rebuilds the view for a stream to include both Parquet files and the hot table.
  /// </summary>
  public async Task RebuildStreamViewAsync(string stream, CancellationToken cancellationToken = default)
  {
    await _dbLock.WaitAsync(cancellationToken);
    try {
      await RebuildStreamViewNoLockAsync(stream, cancellationToken);
    } finally {
      _dbLock.Release();
    }
  }

  private async Task RebuildStreamViewNoLockAsync(string stream, CancellationToken cancellationToken = default)
  {
    var mapping = _parquetManager.GetStreamMapping(stream);
    var hotTableName = GetHotTableName(stream);

    bool hasHotTable;
    lock (_registrationLock) {
      hasHotTable = _hotTableStreams.ContainsKey(stream);
    }

    string viewSql;
    if (mapping != null && mapping.ParquetFiles.Count > 0) {
      var parquetSql = mapping.GetParquetReadSql();
      viewSql = hasHotTable
          ? $"CREATE OR REPLACE VIEW {mapping.GetViewName()} AS SELECT * FROM ({parquetSql} UNION ALL BY NAME SELECT * FROM {hotTableName})"
          : mapping.GetCreateViewSql();
    } else {
      // No Parquet files yet — view reads only from the hot table
      var viewName = StreamTableMapping.EscapeIdentifierPublic(stream);
      viewSql = hasHotTable
          ? $"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM {hotTableName}"
          : $"CREATE OR REPLACE VIEW {viewName} AS SELECT * FROM (SELECT NULL LIMIT 0)";
    }

    await ExecuteNonQueryNoLockAsync(viewSql, cancellationToken);

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
    _dbLock.Dispose();
  }
}