using Lumina.Core.Configuration;
using Lumina.Storage.Wal;

using Microsoft.AspNetCore.Mvc;

namespace Lumina.Query.Endpoints;

/// <summary>
/// Query endpoint for SQL queries over log data.
/// </summary>
public static class QueryEndpoint
{
  public static void MapQueryEndpoints(this IEndpointRouteBuilder app)
  {
    var group = app.MapGroup("/v1/query");

    // GET /v1/query/sql?q={sql} - URL-based queries
    group.MapGet("/sql", ExecuteSqlGetAsync)
        .WithName("ExecuteSQLGet")
        .WithDescription("Execute a SQL query over log data (URL parameter)")
        .Produces<QueryResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest);

    // POST /v1/query/sql (text/plain) - Plain SQL body
    group.MapPost("/sql", ExecuteSqlPostAsync)
        .WithName("ExecuteSQLPost")
        .WithDescription("Execute a SQL query over log data (plain text body)")
        .Produces<QueryResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest);

    // POST /v1/query/sql/parameterized (application/json) - Parameterized queries
    group.MapPost("/sql/parameterized", ExecuteSqlParameterizedAsync)
        .WithName("ExecuteSqlParameterized")
        .WithDescription("Execute a parameterized SQL query over log data")
        .Produces<QueryResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest);

    group.MapGet("/logs/{stream}", QueryLogsAsync)
        .WithName("QueryLogs")
        .WithDescription("Query logs from a specific stream")
        .Produces<QueryResponse>(StatusCodes.Status200OK);

    group.MapGet("/search/{stream}", SearchLogsAsync)
        .WithName("SearchLogs")
        .WithDescription("Full-text search over logs")
        .Produces<QueryResponse>(StatusCodes.Status200OK);

    group.MapGet("/stats/{stream}", GetStatsAsync)
        .WithName("GetStats")
        .WithDescription("Get statistics for a stream")
        .Produces<QueryResponse>(StatusCodes.Status200OK);

    // Stream management endpoints
    var streamsGroup = app.MapGroup("/v1/streams");

    // GET /v1/streams - List all available streams
    streamsGroup.MapGet("/", ListStreamsAsync)
        .WithName("ListStreams")
        .WithDescription("List all available streams")
        .Produces<StreamListResponse>(StatusCodes.Status200OK);

    // GET /v1/streams/{stream}/schema - Get stream schema
    streamsGroup.MapGet("/{stream}/schema", GetStreamSchemaAsync)
        .WithName("GetStreamSchema")
        .WithDescription("Get schema information for a stream")
        .Produces<StreamSchemaResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status404NotFound);
  }

  /// <summary>
  /// GET /v1/query/sql?q={sql} - Execute SQL query from URL parameter.
  /// </summary>
  private static async Task<IResult> ExecuteSqlGetAsync(
      [FromQuery(Name = "q")] string q,
      [FromServices] DuckDbQueryService queryService,
      [FromServices] QuerySettings settings,
      CancellationToken cancellationToken)
  {
    if (string.IsNullOrWhiteSpace(q)) {
      return Results.BadRequest("SQL query (q) is required");
    }

    return await ExecuteQueryInternalAsync(q, null, queryService, settings, cancellationToken);
  }

  /// <summary>
  /// POST /v1/query/sql - Execute SQL query from plain text body.
  /// </summary>
  private static async Task<IResult> ExecuteSqlPostAsync(
      HttpContext context,
      [FromServices] DuckDbQueryService queryService,
      [FromServices] QuerySettings settings,
      CancellationToken cancellationToken)
  {
    using var reader = new StreamReader(context.Request.Body);
    var sql = await reader.ReadToEndAsync(cancellationToken);

    if (string.IsNullOrWhiteSpace(sql)) {
      return Results.BadRequest("SQL query body is required");
    }

    return await ExecuteQueryInternalAsync(sql, null, queryService, settings, cancellationToken);
  }

  /// <summary>
  /// POST /v1/query/sql/parameterized - Execute parameterized SQL query.
  /// </summary>
  private static async Task<IResult> ExecuteSqlParameterizedAsync(
      [FromBody] SqlParameterizedRequest request,
      [FromServices] DuckDbQueryService queryService,
      [FromServices] QuerySettings settings,
      CancellationToken cancellationToken)
  {
    if (string.IsNullOrWhiteSpace(request.Sql)) {
      return Results.BadRequest("SQL query is required");
    }

    return await ExecuteQueryInternalAsync(request.Sql, request.Parameters, queryService, settings, cancellationToken);
  }

  /// <summary>
  /// Internal method to execute queries with validation.
  /// </summary>
  private static async Task<IResult> ExecuteQueryInternalAsync(
      string sql,
      Dictionary<string, object?>? parameters,
      DuckDbQueryService queryService,
      QuerySettings settings,
      CancellationToken cancellationToken)
  {
    // Rewrite single-quoted stream names (e.g. FROM 'my-stream') to properly
    // double-quoted SQL identifiers before validation and execution.
    sql = SqlValidator.RewriteSingleQuotedIdentifiers(sql);

    // Rewrite QuestDB-style TICK interval expressions (e.g. ts IN '$now - 5m..$now')
    // into standard SQL BETWEEN clauses so DuckDB can push filters to Parquet files.
    sql = SqlValidator.RewriteTickIntervals(sql);

    // Validate SQL if validation is enabled
    if (settings.EnableSqlValidation) {
      var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);
      if (!isValid) {
        return Results.BadRequest(error);
      }
    }

    try {
      var result = await queryService.ExecuteQueryAsync(sql, parameters, cancellationToken);

      var columns = result.Columns.Select(c => new EndpointColumnInfo {
        Name = c,
        Type = "UNKNOWN",
        IsNullable = true
      }).ToList();

      return Results.Ok(new QueryResponse {
        Rows = result.Rows,
        RowCount = result.RowCount,
        Columns = columns,
        ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds,
        RegisteredStreams = result.RegisteredStreams
      });
    } catch (Exception ex) {
      return Results.BadRequest($"Query execution failed: {ex.Message}");
    }
  }

  private static async Task<IResult> QueryLogsAsync(
      string stream,
      [FromServices] DuckDbQueryService queryService,
      [FromQuery] DateTime? start,
      [FromQuery] DateTime? end,
      [FromQuery] string? level,
      [FromQuery] int limit = 1000,
      CancellationToken cancellationToken = default)
  {
    var result = await queryService.QueryLogsAsync(stream, start, end, level, limit, cancellationToken);

    var columns = result.Columns.Select(c => new EndpointColumnInfo {
      Name = c,
      Type = "UNKNOWN",
      IsNullable = true
    }).ToList();

    return Results.Ok(new QueryResponse {
      Rows = result.Rows,
      RowCount = result.RowCount,
      Columns = columns,
      ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds,
      RegisteredStreams = result.RegisteredStreams
    });
  }

  private static async Task<IResult> SearchLogsAsync(
      string stream,
      [FromQuery] string q,
      [FromServices] DuckDbQueryService queryService,
      [FromQuery] int limit = 1000,
      CancellationToken cancellationToken = default)
  {
    if (string.IsNullOrWhiteSpace(q)) {
      return Results.BadRequest("Search term (q) is required");
    }

    var result = await queryService.SearchLogsAsync(stream, q, limit, cancellationToken);

    var columns = result.Columns.Select(c => new EndpointColumnInfo {
      Name = c,
      Type = "UNKNOWN",
      IsNullable = true
    }).ToList();

    return Results.Ok(new QueryResponse {
      Rows = result.Rows,
      RowCount = result.RowCount,
      Columns = columns,
      ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds,
      RegisteredStreams = result.RegisteredStreams
    });
  }

  private static async Task<IResult> GetStatsAsync(
      string stream,
      [FromServices] DuckDbQueryService queryService,
      [FromQuery] DateTime? start,
      [FromQuery] DateTime? end,
      CancellationToken cancellationToken = default)
  {
    var result = await queryService.GetStatsAsync(stream, start, end, cancellationToken);

    var columns = result.Columns.Select(c => new EndpointColumnInfo {
      Name = c,
      Type = "UNKNOWN",
      IsNullable = true
    }).ToList();

    return Results.Ok(new QueryResponse {
      Rows = result.Rows,
      RowCount = result.RowCount,
      Columns = columns,
      ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds,
      RegisteredStreams = result.RegisteredStreams
    });
  }

  /// <summary>
  /// GET /v1/streams - List all available streams.
  /// </summary>
  private static async Task<IResult> ListStreamsAsync(
      [FromServices] DuckDbQueryService queryService,
      [FromServices] ParquetManager parquetManager,
      [FromServices] WalHotBuffer hotBuffer,
      CancellationToken cancellationToken)
  {
    var registeredStreams = queryService.GetRegisteredStreams();
    var mappings = parquetManager.GetStreamMappings();
    var hotStreams = hotBuffer.GetBufferedStreams().ToHashSet(StringComparer.OrdinalIgnoreCase);

    // Index Parquet mappings by stream name so we can look them up while iterating
    // the full set of known streams (registered DuckDB views + Parquet-backed streams).
    var mappingIndex = mappings.ToDictionary(m => m.StreamName, m => m, StringComparer.OrdinalIgnoreCase);

    // Union: every stream that has a DuckDB view (hot-only or Parquet-backed).
    var allNames = registeredStreams
        .Union(mappingIndex.Keys, StringComparer.OrdinalIgnoreCase)
        .OrderBy(s => s, StringComparer.OrdinalIgnoreCase);

    var streamInfos = allNames.Select(name => {
      mappingIndex.TryGetValue(name, out var m);
      return new StreamInfo {
        Name = name,
        FileCount = m?.ParquetFiles.Count ?? 0,
        TotalSizeBytes = m?.ParquetFiles.Sum(f => new FileInfo(f).Length) ?? 0L,
        HasHotData = hotStreams.Contains(name)
      };
    }).ToList();

    return Results.Ok(new StreamListResponse {
      Streams = streamInfos,
      TotalCount = streamInfos.Count,
      LastRefreshTime = queryService.LastRefreshTime
    });
  }

  /// <summary>
  /// GET /v1/streams/{stream}/schema - Get stream schema.
  /// </summary>
  private static async Task<IResult> GetStreamSchemaAsync(
      string stream,
      [FromServices] ParquetManager parquetManager,
      CancellationToken cancellationToken)
  {
    var schema = await parquetManager.GetStreamSchemaAsync(stream, cancellationToken);

    if (schema == null) {
      return Results.NotFound($"Stream '{stream}' not found");
    }

    // Convert ColumnInfo from Query namespace to EndpointColumnInfo
    var columns = schema.Columns.Select(c => new EndpointColumnInfo {
      Name = c.Name,
      Type = c.Type,
      IsNullable = c.IsNullable
    }).ToList();

    return Results.Ok(new StreamSchemaResponse {
      StreamName = schema.StreamName,
      Columns = columns,
      FileCount = schema.FileCount,
      TotalSizeBytes = schema.TotalSizeBytes,
      MinTimestamp = schema.MinTimestamp,
      MaxTimestamp = schema.MaxTimestamp
    });
  }
}

/// <summary>
/// SQL query request for parameterized queries.
/// </summary>
public sealed class SqlParameterizedRequest
{
  /// <summary>
  /// Gets or sets the SQL query with parameter placeholders ($name or $1, $2, etc.).
  /// </summary>
  public required string Sql { get; init; }

  /// <summary>
  /// Gets or sets the parameters for the query.
  /// Keys are parameter names (without $ prefix), values are parameter values.
  /// </summary>
  public Dictionary<string, object?>? Parameters { get; init; }
}

/// <summary>
/// Query response with result data and execution metadata.
/// </summary>
public sealed class QueryResponse
{
  /// <summary>
  /// Gets the rows returned by the query.
  /// </summary>
  public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; init; } = Array.Empty<IReadOnlyDictionary<string, object?>>();

  /// <summary>
  /// Gets the number of rows returned.
  /// </summary>
  public int RowCount { get; init; }

  /// <summary>
  /// Gets the column names and their types.
  /// </summary>
  public IReadOnlyList<EndpointColumnInfo> Columns { get; init; } = Array.Empty<EndpointColumnInfo>();

  /// <summary>
  /// Gets the execution time in milliseconds.
  /// </summary>
  public double ExecutionTimeMs { get; init; }

  /// <summary>
  /// Gets the available streams that were registered as tables for this query.
  /// </summary>
  public IReadOnlyList<string> RegisteredStreams { get; init; } = Array.Empty<string>();
}

/// <summary>
/// Information about a result column for API responses.
/// </summary>
public sealed class EndpointColumnInfo
{
  /// <summary>
  /// Gets the column name.
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  /// Gets the column data type.
  /// </summary>
  public required string Type { get; init; }

  /// <summary>
  /// Gets a value indicating whether the column can contain null values.
  /// </summary>
  public bool IsNullable { get; init; } = true;
}

/// <summary>
/// Response for listing streams.
/// </summary>
public sealed class StreamListResponse
{
  /// <summary>
  /// Gets the list of available streams.
  /// </summary>
  public IReadOnlyList<StreamInfo> Streams { get; init; } = Array.Empty<StreamInfo>();

  /// <summary>
  /// Gets the total number of streams.
  /// </summary>
  public int TotalCount { get; init; }

  /// <summary>
  /// Gets the last time streams were refreshed.
  /// </summary>
  public DateTime LastRefreshTime { get; init; }
}

/// <summary>
/// Information about a stream.
/// </summary>
public sealed class StreamInfo
{
  /// <summary>
  /// Gets the stream name.
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  /// Gets the number of Parquet files backing this stream.
  /// Zero for streams that only exist in the live hot buffer.
  /// </summary>
  public int FileCount { get; init; }

  /// <summary>
  /// Gets the total size in bytes of all Parquet files.
  /// </summary>
  public long TotalSizeBytes { get; init; }

  /// <summary>
  /// Gets whether this stream currently has live entries in the hot buffer
  /// (i.e. data ingested since the last compaction cycle).
  /// </summary>
  public bool HasHotData { get; init; }
}

/// <summary>
/// Response for stream schema.
/// </summary>
public sealed class StreamSchemaResponse
{
  /// <summary>
  /// Gets the stream name.
  /// </summary>
  public required string StreamName { get; init; }

  /// <summary>
  /// Gets the columns in the stream schema.
  /// </summary>
  public required IReadOnlyList<EndpointColumnInfo> Columns { get; init; }

  /// <summary>
  /// Gets the number of Parquet files backing this stream.
  /// </summary>
  public int FileCount { get; init; }

  /// <summary>
  /// Gets the total size in bytes of all Parquet files.
  /// </summary>
  public long TotalSizeBytes { get; init; }

  /// <summary>
  /// Gets the earliest timestamp in the stream.
  /// </summary>
  public DateTime? MinTimestamp { get; init; }

  /// <summary>
  /// Gets the latest timestamp in the stream.
  /// </summary>
  public DateTime? MaxTimestamp { get; init; }
}