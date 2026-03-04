# Implementation Plan

[Overview]
Redesign the SQL query endpoint to abstract internal storage details while enabling powerful queries with external data joins.

The implementation creates a "virtual table" layer where streams are automatically registered as DuckDB tables/views, hiding the L1/L2 Parquet file structure. Users can write standard SQL referencing stream names directly (`SELECT * FROM my_stream`). External data files (CSV, JSON, Parquet) can be referenced via URL or file path in queries, leveraging DuckDB's native capabilities. Three endpoint variants support different use cases: GET with URL query parameter, POST with plain text SQL, and POST with parameterized queries for safe template execution.

[Types]

### StreamTableMapping
Represents a mapping between a stream name and its underlying Parquet files.

```csharp
namespace Lumina.Query;

/// <summary>
/// Represents the mapping of a stream to its underlying Parquet files.
/// </summary>
public sealed class StreamTableMapping
{
    /// <summary>
    /// Gets the stream name (used as the table name in SQL).
    /// </summary>
    public required string StreamName { get; init; }
    
    /// <summary>
    /// Gets the list of Parquet file paths for this stream.
    /// </summary>
    public required IReadOnlyList<string> ParquetFiles { get; init; }
    
    /// <summary>
    /// Gets the CREATE VIEW SQL statement for this stream.
    /// </summary>
    public string GetCreateViewSql(string? schema = null)
    {
        var files = ParquetFiles;
        if (files.Count == 0)
        {
            return $"CREATE VIEW IF NOT EXISTS {StreamName} AS SELECT * FROM (SELECT NULL LIMIT 0)";
        }
        
        var fileList = string.Join(", ", files.Select(f => $"'{EscapeSqlString(f)}'"));
        var viewName = schema != null ? $"{schema}.{StreamName}" : StreamName;
        
        return $"CREATE VIEW IF NOT EXISTS {viewName} AS SELECT * FROM read_parquet([{fileList}], union_by_name=true)";
    }
    
    private static string EscapeSqlString(string s) => s.Replace("'", "''");
}
```

### SqlQueryRequest
Request model for parameterized SQL queries.

```csharp
namespace Lumina.Query.Endpoints;

/// <summary>
/// Request for a parameterized SQL query.
/// </summary>
public sealed class SqlQueryRequest
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
```

### QueryResponse (enhanced)
Enhanced response model with query metadata.

```csharp
namespace Lumina.Query.Endpoints;

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
    public IReadOnlyList<ColumnInfo> Columns { get; init; } = Array.Empty<ColumnInfo>();
    
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
/// Information about a result column.
/// </summary>
public sealed class ColumnInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public bool IsNullable { get; init; } = true;
}
```

### StreamSchemaInfo
Schema information for a stream (optional endpoint).

```csharp
namespace Lumina.Query.Endpoints;

/// <summary>
/// Schema information for a stream.
/// </summary>
public sealed class StreamSchemaInfo
{
    /// <summary>
    /// Gets the stream name.
    /// </summary>
    public required string StreamName { get; init; }
    
    /// <summary>
    /// Gets the columns in the stream schema.
    /// </summary>
    public required IReadOnlyList<ColumnInfo> Columns { get; init; }
    
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
```

[Files]

### Files to Modify

1. **Lumina/Query/DuckDbQueryService.cs**
   - Add `RegisterStreamTablesAsync()` method to auto-register streams as views
   - Add `ExecuteQueryAsync()` overload with parameter support
   - Add `GetRegisteredStreams()` method
   - Add `RefreshStreamsAsync()` method to update stream registrations
   - Modify initialization to discover and register streams

2. **Lumina/Query/ParquetManager.cs**
   - Add `DiscoverAllStreams()` method to find all unique stream names
   - Add `GetStreamMappings()` method to return StreamTableMapping objects
   - Add `GetStreamSchemaAsync()` method to infer schema from Parquet files

3. **Lumina/Query/Endpoints/QueryEndpoint.cs**
   - Replace `ExecuteSqlAsync` with three endpoints:
     - `GET /v1/query/sql?q={sql}` - URL-based queries
     - `POST /v1/query/sql` (text/plain) - Plain SQL body
     - `POST /v1/query/sql/parameterized` (application/json) - Parameterized queries
   - Add `GET /v1/streams` - List all available streams
   - Add `GET /v1/streams/{stream}/schema` - Get stream schema

4. **Lumina/Core/Configuration/QuerySettings.cs**
   - Add `RefreshStreamsIntervalSeconds` setting (default: 60)
   - Add `AllowExternalFileAccess` setting (default: true)
   - Add `AllowedExternalProtocols` setting (default: ["http", "https", "file", "s3"])

### New Files to Create

1. **Lumina/Query/StreamTableMapping.cs**
   - Purpose: Define the mapping between stream names and Parquet files
   - Contains SQL generation logic for CREATE VIEW statements

2. **Lumina/Query/SqlValidator.cs**
   - Purpose: Validate SQL queries are SELECT-only (safety check)
   - Allow external file references via DuckDB functions
   - Block dangerous operations (DROP, DELETE, UPDATE, INSERT, CREATE TABLE, etc.)

3. **Lumina/Query/StreamDiscoveryService.cs**
   - Purpose: Background service to discover and refresh stream registrations
   - Runs on a configurable interval
   - Updates DuckDB views when new streams appear

### Test Files to Create

1. **Tests/Query/StreamTableMappingTests.cs**
   - Test CREATE VIEW SQL generation
   - Test escaping of stream names with special characters
   - Test empty stream handling

2. **Tests/Query/SqlValidatorTests.cs**
   - Test SELECT-only validation
   - Test dangerous operation detection
   - Test allowed external file patterns

3. **Tests/Query/StreamDiscoveryServiceTests.cs**
   - Test stream discovery from L1/L2 directories
   - Test refresh behavior

### Test Files to Modify

1. **Tests/Storage/EndToEndTests.cs**
   - Add test for querying via stream name after compaction
   - Add test for joining multiple streams

[Functions]

### New Functions

1. **DuckDbQueryService.RegisterStreamTablesAsync()**
   - File: Lumina/Query/DuckDbQueryService.cs
   - Signature: `Task RegisterStreamTablesAsync(CancellationToken ct = default)`
   - Purpose: Discover all streams and register them as DuckDB views
   - Implementation: Call ParquetManager.GetStreamMappings(), execute CREATE VIEW for each

2. **DuckDbQueryService.ExecuteQueryAsync(string sql, Dictionary<string, object?>? parameters)**
   - File: Lumina/Query/DuckDbQueryService.cs
   - Signature: `Task<QueryResult> ExecuteQueryAsync(string sql, Dictionary<string, object?>? parameters, CancellationToken ct = default)`
   - Purpose: Execute parameterized queries safely
   - Implementation: Use DuckDB prepared statements with parameter binding

3. **DuckDbQueryService.RefreshStreamsAsync()**
   - File: Lumina/Query/DuckDbQueryService.cs
   - Signature: `Task RefreshStreamsAsync(CancellationToken ct = default)`
   - Purpose: Refresh stream registrations (called by background service)

4. **DuckDbQueryService.GetRegisteredStreams()**
   - File: Lumina/Query/DuckDbQueryService.cs
   - Signature: `IReadOnlyList<string> GetRegisteredStreams()`
   - Purpose: Return list of currently registered stream names

5. **ParquetManager.DiscoverAllStreams()**
   - File: Lumina/Query/ParquetManager.cs
   - Signature: `IReadOnlyList<string> DiscoverAllStreams()`
   - Purpose: Scan L1 and L2 directories to find all unique stream names

6. **ParquetManager.GetStreamMappings()**
   - File: Lumina/Query/ParquetManager.cs
   - Signature: `IReadOnlyList<StreamTableMapping> GetStreamMappings()`
   - Purpose: Return mappings for all discovered streams

7. **ParquetManager.GetStreamSchemaAsync()**
   - File: Lumina/Query/ParquetManager.cs
   - Signature: `Task<StreamSchemaInfo?> GetStreamSchemaAsync(string stream, CancellationToken ct = default)`
   - Purpose: Infer schema from Parquet files for a stream

8. **SqlValidator.IsValidSelectQuery()**
   - File: Lumina/Query/SqlValidator.cs
   - Signature: `(bool IsValid, string? Error) IsValidSelectQuery(string sql)`
   - Purpose: Validate SQL is a safe SELECT query

9. **StreamDiscoveryService.StartAsync/ExecuteAsync**
   - File: Lumina/Query/StreamDiscoveryService.cs
   - Purpose: Background service to refresh stream registrations

### Modified Functions

1. **DuckDbQueryService.InitializeAsync()**
   - Current: Opens in-memory connection
   - Change: Also discover and register all streams as views

2. **QueryEndpoint.MapQueryEndpoints()**
   - Current: Maps `/sql`, `/logs/{stream}`, `/search/{stream}`, `/stats/{stream}`
   - Change: Replace `/sql` with three endpoints (GET, POST plain, POST parameterized)
   - Add: `/streams` and `/streams/{stream}/schema` endpoints

3. **DuckDbQueryService.QueryLogsAsync()**
   - Current: Builds SQL with read_parquet([...]) directly
   - Change: Use the registered view instead (optional, for backward compatibility)

[Classes]

### New Classes

1. **StreamTableMapping**
   - File: Lumina/Query/StreamTableMapping.cs
   - Purpose: Encapsulate stream-to-table mapping
   - Key members: StreamName, ParquetFiles, GetCreateViewSql()

2. **SqlValidator**
   - File: Lumina/Query/SqlValidator.cs
   - Purpose: Validate SQL queries for safety
   - Key method: IsValidSelectQuery()
   - Checks: Only SELECT allowed, block DDL/DML operations

3. **StreamDiscoveryService** (IHostedService)
   - File: Lumina/Query/StreamDiscoveryService.cs
   - Purpose: Background service for stream discovery
   - Dependencies: DuckDbQueryService, ParquetManager, ILogger
   - Interval: Configurable via QuerySettings

4. **ColumnInfo**
   - File: Lumina/Query/Endpoints/QueryEndpoint.cs
   - Purpose: Column metadata in responses

5. **StreamSchemaInfo**
   - File: Lumina/Query/Endpoints/QueryEndpoint.cs
   - Purpose: Stream schema response model

### Modified Classes

1. **DuckDbQueryService**
   - File: Lumina/Query/DuckDbQueryService.cs
   - Add: `_registeredStreams` HashSet<string>
   - Add: `_lastRefreshTime` DateTime
   - Add: Stream registration methods

2. **ParquetManager**
   - File: Lumina/Query/ParquetManager.cs
   - Add: Stream discovery methods

3. **QuerySettings**
   - File: Lumina/Core/Configuration/QuerySettings.cs
   - Add: RefreshStreamsIntervalSeconds, AllowExternalFileAccess, AllowedExternalProtocols

4. **QueryResponse**
   - File: Lumina/Query/Endpoints/QueryEndpoint.cs
   - Add: RegisteredStreams property
   - Change: Columns to include type information

[Dependencies]

### No New Packages Required

All functionality uses existing packages:
- DuckDB.NET.Data.Full - already installed
- Parquet.Net - already installed

### Internal Dependencies

- StreamDiscoveryService depends on: DuckDbQueryService, ParquetManager, ILogger, QuerySettings
- SqlValidator has no dependencies (static or singleton)
- DuckDbQueryService depends on: ParquetManager, QuerySettings, ILogger

[Testing]

### Unit Tests Required

1. **StreamTableMappingTests**
   - CREATE VIEW SQL generation for valid streams
   - CREATE VIEW SQL for empty streams (no files)
   - SQL escaping for streams with special characters in name
   - File path escaping in read_parquet() calls

2. **SqlValidatorTests**
   - Valid SELECT queries pass
   - INSERT/UPDATE/DELETE blocked
   - DROP/ALTER/TRUNCATE blocked
   - CREATE TABLE/INDEX blocked
   - Allowed: read_parquet, read_csv, read_json (external files)
   - Allowed: CTEs, subqueries, window functions

3. **ParquetManagerTests** (extend existing)
   - DiscoverAllStreams returns unique stream names
   - GetStreamMappings returns correct file lists

### Integration Tests Required

1. **QueryEndpointIntegrationTests**
   - GET /v1/query/sql?q=... returns results
   - POST /v1/query/sql with text/plain body
   - POST /v1/query/sql/parameterized with parameters
   - Query referencing non-existent stream returns clear error
   - Query with external file join works
   - GET /v1/streams returns all streams
   - GET /v1/streams/{stream}/schema returns schema

2. **StreamRegistrationTests**
   - New stream created after ingestion appears in query
   - Background refresh picks up new streams

[Implementation Order]

1. **Step 1: Create StreamTableMapping and enhance ParquetManager**
   - Create StreamTableMapping.cs with SQL generation logic
   - Add DiscoverAllStreams() to ParquetManager
   - Add GetStreamMappings() to ParquetManager
   - Add unit tests for StreamTableMapping

2. **Step 2: Create SqlValidator**
   - Create SqlValidator.cs with SELECT-only validation
   - Allow DuckDB file-reading functions (read_parquet, read_csv, read_json_auto)
   - Block DDL/DML operations
   - Add unit tests for SqlValidator

3. **Step 3: Enhance DuckDbQueryService**
   - Add stream registration methods
   - Add parameterized query support
   - Modify InitializeAsync to register streams
   - Add GetRegisteredStreams() method

4. **Step 4: Update QueryEndpoint**
   - Add GET /v1/query/sql?q=... endpoint
   - Update POST /v1/query/sql to accept text/plain body
   - Add POST /v1/query/sql/parameterized endpoint
   - Add GET /v1/streams endpoint
   - Add GET /v1/streams/{stream}/schema endpoint
   - Update QueryResponse model

5. **Step 5: Create StreamDiscoveryService**
   - Create StreamDiscoveryService as IHostedService
   - Wire up in Program.cs
   - Add configuration settings

6. **Step 6: Add configuration settings**
   - Update QuerySettings with new properties
   - Add default values and validation

7. **Step 7: Integration tests and documentation**
   - Add integration tests for new endpoints
   - Update API documentation
   - Test external file joins

## Design Decisions Summary

1. **External Data**: Users reference external files directly in SQL using DuckDB's native functions (`read_csv('url')`, `read_json('path')`, etc.). No upload endpoint needed.

2. **Query API**: Streams auto-register as tables. Users write standard SQL: `SELECT * FROM my_stream JOIN read_csv('https://...') AS lookup ON ...`

3. **Security**: Allow any SELECT query. SqlValidator blocks DDL/DML but allows all read operations including external file access.

4. **Endpoints**:
   - `GET /v1/query/sql?q={sql}` - URL query (for quick ad-hoc queries)
   - `POST /v1/query/sql` (text/plain) - Plain SQL body
   - `POST /v1/query/sql/parameterized` (application/json) - Parameterized queries
   - `GET /v1/streams` - List available streams
   - `GET /v1/streams/{stream}/schema` - Get stream schema

## Example Usage

```bash
# List available streams
curl http://localhost:8080/v1/streams

# Query a stream
curl "http://localhost:8080/v1/query/sql?q=SELECT%20*%20FROM%20my_app_logs%20WHERE%20level%20%3D%20%27error%27"

# Query with POST
curl -X POST http://localhost:8080/v1/query/sql \
  -H "Content-Type: text/plain" \
  -d "SELECT level, COUNT(*) FROM my_app_logs GROUP BY level"

# Parameterized query
curl -X POST http://localhost:8080/v1/query/sql/parameterized \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM my_app_logs WHERE level = $level", "parameters": {"level": "error"}}'

# Join with external CSV
curl -X POST http://localhost:8080/v1/query/sql \
  -H "Content-Type: text/plain" \
  -d "SELECT l.*, s.service_name FROM my_app_logs l JOIN read_csv('https://my-bucket/services.csv') s ON l.service_id = s.id"