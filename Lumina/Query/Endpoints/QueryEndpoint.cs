using Lumina.Core.Configuration;
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
        
        group.MapPost("/sql", ExecuteSqlAsync)
            .WithName("ExecuteSQL")
            .WithDescription("Execute a SQL query over log data")
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
    }
    
    private static async Task<IResult> ExecuteSqlAsync(
        [FromBody] SqlQueryRequest request,
        [FromServices] DuckDbQueryService queryService,
        [FromServices] QuerySettings settings,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Sql))
        {
            return Results.BadRequest("SQL query is required");
        }
        
        // Basic validation
        var sql = request.Sql.Trim();
        if (!sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
        {
            return Results.BadRequest("Only SELECT queries are allowed");
        }
        
        try
        {
            var result = await queryService.ExecuteQueryAsync(sql, cancellationToken);
            
            return Results.Ok(new QueryResponse
            {
                Rows = result.Rows,
                RowCount = result.RowCount,
                Columns = result.Columns,
                ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds
            });
        }
        catch (Exception ex)
        {
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
        
        return Results.Ok(new QueryResponse
        {
            Rows = result.Rows,
            RowCount = result.RowCount,
            Columns = result.Columns,
            ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds
        });
    }
    
    private static async Task<IResult> SearchLogsAsync(
        string stream,
        [FromQuery] string q,
        [FromServices] DuckDbQueryService queryService,
        [FromQuery] int limit = 1000,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(q))
        {
            return Results.BadRequest("Search term (q) is required");
        }
        
        var result = await queryService.SearchLogsAsync(stream, q, limit, cancellationToken);
        
        return Results.Ok(new QueryResponse
        {
            Rows = result.Rows,
            RowCount = result.RowCount,
            Columns = result.Columns,
            ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds
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
        
        return Results.Ok(new QueryResponse
        {
            Rows = result.Rows,
            RowCount = result.RowCount,
            Columns = result.Columns,
            ExecutionTimeMs = result.ExecutionTime.TotalMilliseconds
        });
    }
}

/// <summary>
/// SQL query request.
/// </summary>
public sealed class SqlQueryRequest
{
    /// <summary>
    /// Gets or sets the SQL query.
    /// </summary>
    public required string Sql { get; init; }
}

/// <summary>
/// Query response.
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
    /// Gets the column names.
    /// </summary>
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
    
    /// <summary>
    /// Gets the execution time in milliseconds.
    /// </summary>
    public double ExecutionTimeMs { get; init; }
}