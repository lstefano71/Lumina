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
            SELECT * FROM read_parquet([{fileList}],union_by_name=true)
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
            SELECT * FROM read_parquet([{fileList}],union_by_name=true)
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
            FROM read_parquet([{fileList}],union_by_name=true)
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