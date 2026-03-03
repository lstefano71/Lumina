# Implementation Plan

[Overview]
Lumina is a high-performance, append-only log storage and retrieval engine for observability and event streaming with tiered storage (WAL hot storage + Parquet cold storage).

This implementation builds a complete .NET 10 monolith application that ingests logs via HTTP/2 Cleartext (h2c) and gRPC (OTLP), persists them to a crash-resilient Write-Ahead Log (WAL), compacts WAL entries to Parquet files for cold storage, and provides SQL query capabilities through embedded DuckDB. The system is designed for high-throughput (500+ MB/s), zero-data-loss durability, and intelligent corruption recovery using sync markers and CRC validation.

[Types]

The type system consists of binary format structures, domain models, and configuration types.

### WAL Binary Format Constants

```csharp
namespace Lumina.Storage.Wal;

/// <summary>
/// WAL file magic bytes: "LUMI" in ASCII
/// </summary>
public static class WalFormat
{
    public const uint Magic = 0x494D554C; // "LUMI" little-endian
    public const byte CurrentVersion = 0x01;
    public const uint SyncMarker = 0x0CB0CEFA; // 0xFA 0xCE 0xB0 0x0C little-endian
}

/// <summary>
/// Entry types in the WAL frame
/// </summary>
public enum WalEntryType : byte
{
    StandardLog = 0x01,
    Metric = 0x02,
    Trace = 0x03
}
```

### WAL File Header Structure

```csharp
namespace Lumina.Storage.Wal;

/// <summary>
/// WAL file header - 8 bytes total
/// </summary>
public readonly struct WalFileHeader
{
    public readonly uint Magic;        // 4 bytes - "LUMI"
    public readonly byte Version;      // 1 byte - format version
    public readonly byte Flags;        // 1 byte - reserved (compression, encryption)
    public readonly ushort Reserved;   // 2 bytes - future use
    
    public const int Size = 8;
    public const uint ExpectedMagic = 0x494D554C;
}
```

### WAL Entry Frame Header Structure

```csharp
namespace Lumina.Storage.Wal;

/// <summary>
/// WAL entry frame header - 14 bytes before payload
/// </summary>
public readonly struct WalFrameHeader
{
    public readonly uint SyncMarker;   // 4 bytes - 0xFA 0xCE 0xB0 0x0C
    public readonly uint Length;       // 4 bytes - payload size
    public readonly uint InvertedLength; // 4 bytes - bitwise NOT of Length (FEC)
    public readonly WalEntryType Type; // 1 byte - entry type
    public readonly byte HeaderCrc;    // 1 byte - CRC-8 of Length, InvertedLength, Type
    
    public const int Size = 14;
    
    public bool IsValid => SyncMarker == WalFormat.SyncMarker && Length == ~InvertedLength;
}
```

### Domain Models

```csharp
namespace Lumina.Core.Models;

/// <summary>
/// Represents a normalized log entry in the unified internal format
/// </summary>
public sealed class LogEntry
{
    public required string Stream { get; init; }
    public required DateTime Timestamp { get; init; }
    public required string Level { get; init; }
    public required string Message { get; init; }
    public Dictionary<string, object?> Attributes { get; init; } = new();
    public string? TraceId { get; init; }
    public string? SpanId { get; init; }
    public int? DurationMs { get; init; }
}

/// <summary>
/// Represents a stream identifier and its metadata
/// </summary>
public sealed class StreamInfo
{
    public required string Name { get; init; }
    public DateTime CreatedAt { get; init; }
    public long TotalEntries { get; set; }
    public long TotalSizeBytes { get; set; }
    public DateTime? LastEntryAt { get; set; }
    public RetentionPolicy? Retention { get; set; }
}

/// <summary>
/// Retention policy for a stream
/// </summary>
public sealed class RetentionPolicy
{
    public int TtlDays { get; init; } = 30;
    public long MaxSizeBytes { get; init; } = long.MaxValue;
}

/// <summary>
/// Compaction cursor tracking progress
/// </summary>
public sealed class CompactionCursor
{
    public required string Stream { get; init; }
    public string? CurrentWalFile { get; set; }
    public long LastCompactedOffset { get; set; }
    public DateTime LastCompactionTime { get; set; }
    public string? LastParquetFile { get; set; }
}
```

### Configuration Types

```csharp
namespace Lumina.Core.Configuration;

/// <summary>
/// Main application configuration
/// </summary>
public sealed class LuminaSettings
{
    public WalSettings Wal { get; init; } = new();
    public CompactionSettings Compaction { get; init; } = new();
    public QuerySettings Query { get; init; } = new();
    public IngestionSettings Ingestion { get; init; } = new();
    public DeploymentMode DeploymentMode { get; init; } = DeploymentMode.Secure;
}

public enum DeploymentMode
{
    Secure,      // HTTPS, HTTP/3, TLS required
    Intranet     // HTTP/2 Cleartext (h2c), HTTP/1.1, no TLS
}

public sealed class WalSettings
{
    public string DataDirectory { get; init; } = "data";
    public long MaxWalSizeBytes { get; init; } = 100 * 1024 * 1024; // 100 MB
    public bool EnableWriteThrough { get; init; } = true;
    public int FlushIntervalMs { get; init; } = 100;
}

public sealed class CompactionSettings
{
    public int IntervalMinutes { get; init; } = 10;
    public int L1IntervalMinutes { get; init; } = 10;  // WAL -> Parquet
    public int L2IntervalHours { get; init; } = 24;    // Daily consolidation
    public int MaxDynamicKeys { get; init; } = 100;    // Keys overflow threshold
    public string ParquetOutputDirectory { get; init; } = "data/parquet";
}

public sealed class QuerySettings
{
    public int MaxResults { get; init; } = 10000;
    public TimeSpan QueryTimeout { get; init; } = TimeSpan.FromSeconds(30);
}

public sealed class IngestionSettings
{
    public int HttpPort { get; init; } = 5000;
    public int MaxRequestBodySize { get; init; } = 10 * 1024 * 1024; // 10 MB
}
```

### Ingestion Request/Response Types

```csharp
namespace Lumina.Ingestion.Models;

/// <summary>
/// JSON ingestion request for a single log entry
/// </summary>
public sealed class LogIngestRequest
{
    public required string Stream { get; init; }
    public required DateTime Timestamp { get; init; }
    public required string Level { get; init; }
    public required string Message { get; init; }
    public Dictionary<string, object?>? Attributes { get; init; }
    public string? TraceId { get; init; }
    public string? SpanId { get; init; }
    public int? DurationMs { get; init; }
}

/// <summary>
/// Batch ingestion request
/// </summary>
public sealed class BatchLogIngestRequest
{
    public required string Stream { get; init; }
    public required IReadOnlyList<LogIngestRequest> Entries { get; init; }
}

/// <summary>
/// Ingestion response
/// </summary>
public sealed class IngestResponse
{
    public bool Success { get; init; }
    public int EntriesAccepted { get; init; }
    public string? Error { get; init; }
}
```

### Query Types

```csharp
namespace Lumina.Query.Models;

/// <summary>
/// Query request parameters
/// </summary>
public sealed class QueryRequest
{
    public required string Stream { get; init; }
    public string? Query { get; init; }  // SQL-like or raw SQL
    public DateTime? StartTime { get; init; }
    public DateTime? EndTime { get; init; }
    public int? Limit { get; init; }
    public int? Offset { get; init; }
}

/// <summary>
/// Query result
/// </summary>
public sealed class QueryResult
{
    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Rows { get; init; } = Array.Empty<IReadOnlyDictionary<string, object?>>();
    public int TotalCount { get; init; }
    public TimeSpan ExecutionTime { get; init; }
    public IReadOnlyList<string> Columns { get; init; } = Array.Empty<string>();
}
```

[Files]

The implementation follows a single monolith project structure with clear domain organization.

### Project Structure

```
Lumina/
├── Lumina.csproj                          # Main project file
├── Program.cs                              # Application entry point
├── appsettings.json                        # Configuration
├── appsettings.Development.json            # Development overrides
│
├── Core/
│   ├── Models/                             # Domain models
│   │   ├── LogEntry.cs
│   │   ├── StreamInfo.cs
│   │   ├── RetentionPolicy.cs
│   │   └── CompactionCursor.cs
│   ├── Configuration/
│   │   ├── LuminaSettings.cs
│   │   ├── WalSettings.cs
│   │   ├── CompactionSettings.cs
│   │   ├── QuerySettings.cs
│   │   └── IngestionSettings.cs
│   └── Constants/
│       └── WellKnown.cs                    # Well-known attribute keys, levels, etc.
│
├── Storage/
│   ├── Wal/
│   │   ├── WalFormat.cs                    # Binary format constants
│   │   ├── WalFileHeader.cs                # File header struct
│   │   ├── WalFrameHeader.cs               # Frame header struct
│   │   ├── WalWriter.cs                    # WAL writing implementation
│   │   ├── WalReader.cs                    # WAL reading with SIMD scanning
│   │   ├── WalEntry.cs                     # Entry representation
│   │   ├── WalManager.cs                   # WAL file management
│   │   └── Crc8.cs                         # CRC-8 implementation
│   ├── Parquet/
│   │   ├── ParquetWriter.cs                # Parquet file writer
│   │   ├── ParquetReader.cs                # Parquet file reader
│   │   ├── SchemaResolver.cs               # Union schema resolution
│   │   ├── TypePromotion.cs                # Type conflict resolution
│   │   └── ParquetManager.cs               # Parquet file management
│   └── Serialization/
│       ├── LogEntrySerializer.cs           # MessagePack serialization
│       └── LogEntryDeserializer.cs         # MessagePack deserialization
│
├── Ingestion/
│   ├── Endpoints/
│   │   ├── JsonIngestionEndpoint.cs        # HTTP JSON ingestion
│   │   └── OtlpIngestionEndpoint.cs        # gRPC OTLP ingestion
│   ├── Normalization/
│   │   ├── INormalizer.cs                  # Normalizer interface
│   │   ├── JsonNormalizer.cs               # JSON to LogEntry
│   │   └── OtlpNormalizer.cs               # OTLP to LogEntry
│   └── Models/
│       ├── LogIngestRequest.cs
│       ├── BatchLogIngestRequest.cs
│       └── IngestResponse.cs
│
├── Compaction/
│   ├── CompactorService.cs                 # Background hosted service
│   ├── L1Compactor.cs                      # WAL -> Parquet compaction
│   ├── L2Compactor.cs                      # Daily/Weekly consolidation
│   └── CursorManager.cs                    # Compaction cursor tracking
│
├── Query/
│   ├── Services/
│   │   └── QueryService.cs                 # DuckDB query execution
│   ├── Endpoints/
│   │   └── QueryEndpoint.cs                # HTTP query endpoint
│   └── Models/
│       ├── QueryRequest.cs
│       └── QueryResult.cs
│
├── Observability/
│   ├── Metrics/
│   │   └── LuminaMetrics.cs                # Prometheus metrics
│   └── Health/
│       └── HealthChecks.cs                 # Health check endpoints
│
└── Protos/
    └── otlp.proto                          # OTLP protobuf definitions (simplified)

Tests/
├── Lumina.Tests.csproj
├── Storage/
│   ├── WalWriterTests.cs
│   ├── WalReaderTests.cs
│   ├── WalCorruptionRecoveryTests.cs
│   └── ParquetWriterTests.cs
├── Ingestion/
│   ├── JsonIngestionTests.cs
│   └── OtlpIngestionTests.cs
├── Compaction/
│   ├── L1CompactorTests.cs
│   └── SchemaResolverTests.cs
├── Query/
│   └── QueryServiceTests.cs
└── Integration/
    ├── EndToEndTests.cs
    └── WalFuzzTests.cs                     # Corruption recovery tests
```

### New Files to Create

| File Path | Purpose |
|-----------|---------|
| `Lumina/Lumina.csproj` | Main project with .NET 10 SDK, dependencies |
| `Lumina/Program.cs` | Application bootstrap, Kestrel configuration |
| `Lumina/appsettings.json` | Default configuration |
| `Lumina/appsettings.Development.json` | Development overrides |
| All Core/ files | Domain models and configuration types |
| All Storage/ files | WAL and Parquet implementation |
| All Ingestion/ files | HTTP endpoints and normalization |
| All Compaction/ files | Background compaction services |
| All Query/ files | DuckDB integration and query API |
| All Observability/ files | Metrics and health checks |
| `Tests/Lumina.Tests.csproj` | Test project |
| All Tests/ files | Unit and integration tests |

[Functions]

### New Functions

#### Storage/WAL Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `WalWriter.WriteAsync` | `ValueTask WriteAsync(LogEntry entry, CancellationToken ct)` | Write a log entry to WAL with fsync |
| `WalWriter.WriteBatchAsync` | `ValueTask WriteBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken ct)` | Batch write with single fsync |
| `WalWriter.FlushAsync` | `ValueTask FlushAsync(CancellationToken ct)` | Force flush buffer to disk |
| `WalWriter.DisposeAsync` | `ValueTask DisposeAsync()` | Close file handle, ensure final flush |
| `WalReader.ReadEntriesAsync` | `IAsyncEnumerable<WalEntry> ReadEntriesAsync(CancellationToken ct)` | Stream all valid entries from WAL |
| `WalReader.ScanToNextMarker` | `bool ScanToNextMarker(ref Span<byte> buffer, ref int offset)` | SIMD-optimized sync marker search |
| `WalReader.ValidateFrameHeader` | `bool ValidateFrameHeader(ReadOnlySpan<byte> header, out WalFrameHeader frame)` | CRC-8 validation of frame header |
| `WalManager.GetOrCreateWriter` | `WalWriter GetOrCreateWriter(string stream)` | Get or create WAL writer for stream |
| `WalManager.RotateWalIfNeeded` | `Task<string?> RotateWalIfNeeded(string stream)` | Rotate WAL when size threshold exceeded |
| `Crc8.Compute` | `byte Compute(ReadOnlySpan<byte> data)` | Calculate CRC-8 checksum |

#### Storage/Parquet Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `ParquetWriter.WriteBatchAsync` | `Task WriteBatchAsync(IReadOnlyList<LogEntry> entries, string outputPath, CancellationToken ct)` | Write entries to Parquet file |
| `ParquetReader.ReadAsync` | `IAsyncEnumerable<LogEntry> ReadAsync(string filePath, CancellationToken ct)` | Read entries from Parquet file |
| `SchemaResolver.ResolveSchema` | `ParquetSchema ResolveSchema(IReadOnlyList<LogEntry> sample)` | Determine union schema from sample |
| `TypePromotion.PromoteType` | `SchemaType PromoteType(SchemaType? current, object? newValue)` | Promote column type for conflicts |
| `ParquetManager.GetParquetFiles` | `IReadOnlyList<string> GetParquetFiles(string stream, DateTime? start, DateTime? end)` | List Parquet files for stream |

#### Storage/Serialization Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `LogEntrySerializer.Serialize` | `byte[] Serialize(LogEntry entry)` | Serialize LogEntry to MessagePack |
| `LogEntrySerializer.SerializeBatch` | `byte[] SerializeBatch(IReadOnlyList<LogEntry> entries)` | Serialize batch efficiently |
| `LogEntryDeserializer.Deserialize` | `LogEntry Deserialize(ReadOnlySpan<byte> data)` | Deserialize MessagePack to LogEntry |

#### Ingestion Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `JsonNormalizer.Normalize` | `LogEntry Normalize(LogIngestRequest request)` | Convert JSON request to LogEntry |
| `JsonNormalizer.NormalizeBatch` | `IReadOnlyList<LogEntry> NormalizeBatch(BatchLogIngestRequest request)` | Batch normalization |
| `OtlpNormalizer.Normalize` | `LogEntry Normalize(ExportLogsServiceRequest otlpRequest)` | Convert OTLP to LogEntry |
| `JsonIngestionEndpoint.HandleSingle` | `Task<IResult> HandleSingle(LogIngestRequest request)` | Single log ingestion endpoint |
| `JsonIngestionEndpoint.HandleBatch` | `Task<IResult> HandleBatch(BatchLogIngestRequest request)` | Batch ingestion endpoint |
| `OtlpIngestionEndpoint.Export` | `Task<ExportLogsServiceResponse> Export(ExportLogsServiceRequest request)` | OTLP gRPC endpoint |

#### Compaction Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `CompactorService.ExecuteAsync` | `Task ExecuteAsync(CancellationToken stoppingToken)` | Background service main loop |
| `L1Compactor.CompactAsync` | `Task<CompactionResult> CompactAsync(string stream, CancellationToken ct)` | WAL -> Parquet compaction |
| `L2Compactor.ConsolidateAsync` | `Task<ConsolidationResult> ConsolidateAsync(string stream, DateTime date, CancellationToken ct)` | Daily consolidation |
| `CursorManager.LoadCursor` | `CompactionCursor? LoadCursor(string stream)` | Load compaction progress |
| `CursorManager.SaveCursor` | `Task SaveCursor(CompactionCursor cursor)` | Persist compaction progress |

#### Query Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `QueryService.ExecuteQueryAsync` | `Task<QueryResult> ExecuteQueryAsync(QueryRequest request, CancellationToken ct)` | Execute SQL query via DuckDB |
| `QueryService.BuildSql` | `string BuildSql(QueryRequest request, IReadOnlyList<string> parquetFiles)` | Translate request to SQL |
| `QueryEndpoint.HandleQuery` | `Task<IResult> HandleQuery(QueryRequest request)` | HTTP query endpoint |

#### Observability Functions

| Function | Signature | Purpose |
|----------|-----------|---------|
| `LuminaMetrics.IncrementWalCorruptions` | `void IncrementWalCorruptions()` | Increment corruption counter |
| `LuminaMetrics.RecordIngestionBytes` | `void RecordIngestionBytes(long bytes)` | Record ingestion rate |
| `LuminaMetrics.SetCompactionLag` | `void SetCompactionLag(TimeSpan lag)` | Set compaction lag gauge |

[Classes]

### New Classes

#### Storage Classes

| Class | File Path | Purpose | Key Members |
|-------|-----------|---------|-------------|
| `WalWriter` | `Storage/Wal/WalWriter.cs` | Writes log entries to WAL file | `WriteAsync`, `WriteBatchAsync`, `FlushAsync`, `DisposeAsync` |
| `WalReader` | `Storage/Wal/WalReader.cs` | Reads WAL with corruption recovery | `ReadEntriesAsync`, `ScanToNextMarker`, `ValidateFrameHeader` |
| `WalManager` | `Storage/Wal/WalManager.cs` | Manages WAL files per stream | `GetOrCreateWriter`, `RotateWalIfNeeded`, `GetWalFiles` |
| `WalEntry` | `Storage/Wal/WalEntry.cs` | Represents a WAL entry with metadata | `Stream`, `Timestamp`, `Payload`, `Type`, `Offset` |
| `Crc8` | `Storage/Wal/Crc8.cs` | CRC-8 implementation for frame headers | `Compute` |
| `ParquetWriter` | `Storage/Parquet/ParquetWriter.cs` | Writes entries to Parquet format | `WriteBatchAsync` |
| `ParquetReader` | `Storage/Parquet/ParquetReader.cs` | Reads Parquet files | `ReadAsync` |
| `SchemaResolver` | `Storage/Parquet/SchemaResolver.cs` | Resolves union schemas | `ResolveSchema`, `GetOverflowKeys` |
| `TypePromotion` | `Storage/Parquet/TypePromotion.cs` | Handles type conflicts | `PromoteType`, `CanPromote` |
| `ParquetManager` | `Storage/Parquet/ParquetManager.cs` | Manages Parquet files | `GetParquetFiles`, `DeleteOldFiles` |
| `LogEntrySerializer` | `Storage/Serialization/LogEntrySerializer.cs` | MessagePack serialization | `Serialize`, `SerializeBatch` |
| `LogEntryDeserializer` | `Storage/Serialization/LogEntryDeserializer.cs` | MessagePack deserialization | `Deserialize`, `DeserializeBatch` |

#### Ingestion Classes

| Class | File Path | Purpose | Key Members |
|-------|-----------|---------|-------------|
| `JsonNormalizer` | `Ingestion/Normalization/JsonNormalizer.cs` | Normalizes JSON requests | `Normalize`, `NormalizeBatch` |
| `OtlpNormalizer` | `Ingestion/Normalization/OtlpNormalizer.cs` | Normalizes OTLP requests | `Normalize`, `NormalizeResourceLogs` |
| `JsonIngestionEndpoint` | `Ingestion/Endpoints/JsonIngestionEndpoint.cs` | HTTP JSON endpoints | `MapEndpoints` (static), `HandleSingle`, `HandleBatch` |
| `OtlpIngestionEndpoint` | `Ingestion/Endpoints/OtlpIngestionEndpoint.cs` | gRPC OTLP endpoint | `Export` |

#### Compaction Classes

| Class | File Path | Purpose | Key Members |
|-------|-----------|---------|-------------|
| `CompactorService` | `Compaction/CompactorService.cs` | Background hosted service | `ExecuteAsync`, `TriggerCompaction` |
| `L1Compactor` | `Compaction/L1Compactor.cs` | WAL to Parquet compaction | `CompactAsync` |
| `L2Compactor` | `Compaction/L2Compactor.cs` | Daily consolidation | `ConsolidateAsync` |
| `CursorManager` | `Compaction/CursorManager.cs` | Tracks compaction progress | `LoadCursor`, `SaveCursor` |

#### Query Classes

| Class | File Path | Purpose | Key Members |
|-------|-----------|---------|-------------|
| `QueryService` | `Query/Services/QueryService.cs` | DuckDB query execution | `ExecuteQueryAsync`, `BuildSql` |
| `QueryEndpoint` | `Query/Endpoints/QueryEndpoint.cs` | HTTP query endpoint | `MapEndpoints` (static), `HandleQuery` |

#### Observability Classes

| Class | File Path | Purpose | Key Members |
|-------|-----------|---------|-------------|
| `LuminaMetrics` | `Observability/Metrics/LuminaMetrics.cs` | Prometheus metrics | `WalCorruptions`, `IngestionRate`, `CompactionLag` |
| `HealthChecks` | `Observability/Health/HealthChecks.cs` | Health check endpoints | `MapHealthEndpoints` (static) |

### Modified Classes

No existing classes to modify - this is a greenfield implementation.

### Removed Classes

None - this is a greenfield implementation.

[Dependencies]

### NuGet Packages

| Package | Version | Purpose |
|---------|---------|---------|
| `Apache.Arrow` | Latest stable | Parquet file format, Arrow integration |
| `Apache.Arrow.Ipc` | Latest stable | Arrow IPC format for DuckDB interop |
| `DuckDB.NET.Data` | Latest stable | Embedded OLAP engine |
| `MessagePack` | 3.x | High-performance binary serialization |
| `Google.Protobuf` | Latest stable | Protocol Buffers runtime |
| `Grpc.AspNetCore` | 2.x | gRPC server support |
| `Microsoft.AspNetCore.OpenApi` | Latest stable | OpenAPI/Swagger generation |
| `Swashbuckle.AspNetCore` | Latest stable | Swagger UI |

### .NET SDK Requirements

- .NET 10 SDK (version 10.0.103 or later)
- C# 14 language features (required for specific patterns)

### Project File Configuration

```xml
<Project Sdk="Microsoft.NET.Sdk.Web">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <InvariantGlobalization>false</InvariantGlobalization>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Apache.Arrow" Version="*" />
    <PackageReference Include="DuckDB.NET.Data" Version="*" />
    <PackageReference Include="MessagePack" Version="3.*" />
    <PackageReference Include="Google.Protobuf" Version="*" />
    <PackageReference Include="Grpc.AspNetCore" Version="2.*" />
    <PackageReference Include="Microsoft.AspNetCore.OpenApi" Version="*" />
    <PackageReference Include="Swashbuckle.AspNetCore" Version="*" />
  </ItemGroup>
</Project>
```

### Test Project Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `Microsoft.NET.Test.Sdk` | Latest | Test framework SDK |
| `xunit` | 2.x | Testing framework |
| `xunit.runner.visualstudio` | 2.x | VS test runner |
| `FluentAssertions` | 7.x | Assertion library |
| `Moq` | 4.x | Mocking framework |
| `Testcontainers` | Latest | Container-based integration tests (optional) |

[Testing]

### Test File Requirements

#### Unit Tests

| Test File | Coverage Target | Key Test Cases |
|-----------|-----------------|----------------|
| `WalWriterTests.cs` | `WalWriter` | Write single entry, write batch, flush, file header validation |
| `WalReaderTests.cs` | `WalReader` | Read all entries, SIMD marker scanning, CRC validation |
| `WalCorruptionRecoveryTests.cs` | `WalReader` | Corrupt sync marker, corrupt length, corrupt payload, re-sync logic |
| `ParquetWriterTests.cs` | `ParquetWriter` | Write batch, schema resolution, file naming |
| `SchemaResolverTests.cs` | `SchemaResolver` | Type promotion (Int→String, Float→Double), overflow keys |
| `LogEntrySerializerTests.cs` | `LogEntrySerializer` | Round-trip serialization, null handling, complex attributes |
| `JsonNormalizerTests.cs` | `JsonNormalizer` | Field mapping, timestamp parsing, attribute extraction |
| `OtlpNormalizerTests.cs` | `OtlpNormalizer` | OTLP structure parsing, resource/scope extraction |
| `QueryServiceTests.cs` | `QueryService` | SQL generation, time range filtering, result mapping |

#### Integration Tests

| Test File | Coverage Target | Key Test Cases |
|-----------|-----------------|----------------|
| `EndToEndTests.cs` | Full pipeline | Ingest → WAL → Compact → Query |
| `WalFuzzTests.cs` | Corruption recovery | Random bit flips, partial writes, truncated files |
| `ConcurrentIngestionTests.cs` | Thread safety | Parallel ingestion, file rotation during writes |
| `CompactionIntegrationTests.cs` | Full compaction | WAL → Parquet → cursor update → WAL deletion |

### Validation Strategies

1. **Binary Format Validation**: Hand-crafted binary files with known content for WAL format testing
2. **Fuzz Testing**: Generate random corruption patterns and verify recovery
3. **Performance Benchmarks**: BenchmarkDotNet tests for ingestion throughput (target: 500 MB/s)
4. **Property-Based Testing**: FsCheck or similar for schema resolution edge cases

### Test Infrastructure

```csharp
// Example test base class
public abstract class WalTestBase : IDisposable
{
    protected readonly string TempDirectory;
    
    protected WalTestBase()
    {
        TempDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(TempDirectory);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(TempDirectory))
            Directory.Delete(TempDirectory, recursive: true);
    }
    
    protected string GetWalPath(string stream) => 
        Path.Combine(TempDirectory, $"{stream}.wal");
}
```

[Implementation Order]

The implementation follows a phased approach matching the PRD structure, ensuring each phase builds on validated foundations.

### Phase 1: Core Storage (The "Spine")

1. **Step 1.1**: Create solution structure
   - Create `Lumina.csproj` with dependencies
   - Create `appsettings.json` and `appsettings.Development.json`
   - Create directory structure

2. **Step 1.2**: Implement WAL binary format types
   - Create `WalFormat.cs` with constants
   - Create `WalFileHeader.cs` struct
   - Create `WalFrameHeader.cs` struct
   - Create `Crc8.cs` implementation

3. **Step 1.3**: Implement domain models
   - Create `LogEntry.cs`
   - Create `StreamInfo.cs`, `RetentionPolicy.cs`, `CompactionCursor.cs`
   - Create configuration classes in `Core/Configuration/`

4. **Step 1.4**: Implement serialization
   - Create `LogEntrySerializer.cs` with MessagePack
   - Create `LogEntryDeserializer.cs`
   - Add MessagePack formatters for custom types if needed

5. **Step 1.5**: Implement `WalWriter`
   - Create file with header
   - Implement frame writing with CRC
   - Implement `WriteThrough` and flush logic
   - Handle file rotation

6. **Step 1.6**: Implement `WalReader`
   - Implement SIMD sync marker scanning
   - Implement frame header validation (CRC-8)
   - Implement payload validation (CRC-32)
   - Implement re-sync on corruption

7. **Step 1.7**: Implement `WalManager`
   - Manage writers per stream
   - Handle WAL rotation
   - Track file sizes

8. **Step 1.8**: Create basic Program.cs with Kestrel
   - Configure HTTP/2 Cleartext (h2c) support
   - Configure HTTP/1.1 fallback
   - Basic dependency injection setup

9. **Step 1.9**: Implement JSON ingestion endpoint
   - Create `JsonNormalizer.cs`
   - Create `JsonIngestionEndpoint.cs`
   - Map POST `/v1/logs` and `/v1/logs/batch`

10. **Step 1.10**: Add unit tests for Phase 1
    - `WalWriterTests.cs`
    - `WalReaderTests.cs`
    - `WalCorruptionRecoveryTests.cs`
    - `LogEntrySerializerTests.cs`

### Phase 2: Compaction & Parquet

11. **Step 2.1**: Implement Parquet schema types
    - Create `SchemaResolver.cs`
    - Create `TypePromotion.cs` with conflict resolution rules

12. **Step 2.2**: Implement `ParquetWriter`
    - Create `ParquetWriter.cs` using Apache.Arrow
    - Implement schema resolution from log batch
    - Handle `_meta` overflow column
    - Generate idempotent file names

13. **Step 2.3**: Implement `ParquetReader`
    - Create `ParquetReader.cs`
    - Read Parquet files back to `LogEntry` objects

14. **Step 2.4**: Implement `CursorManager`
    - Create `CursorManager.cs`
    - Load/save `cursor.json` per stream

15. **Step 2.5**: Implement `L1Compactor`
    - Create `L1Compactor.cs`
    - Read WAL entries
    - Write Parquet files
    - Update cursor
    - Delete compacted WAL segments

16. **Step 2.6**: Implement `CompactorService`
    - Create `CompactorService.cs` as `IHostedService`
    - Time-based trigger (every 10 minutes)
    - Size-based trigger (WAL > 100MB)

17. **Step 2.7**: Implement `ParquetManager`
    - Create `ParquetManager.cs`
    - List files by stream and time range
    - Implement retention policy enforcement

18. **Step 2.8**: Add unit tests for Phase 2
    - `ParquetWriterTests.cs`
    - `SchemaResolverTests.cs`
    - `L1CompactorTests.cs`

### Phase 3: Query & OTLP

19. **Step 3.1**: Implement `QueryService`
    - Create `QueryService.cs`
    - Integrate DuckDB.NET
    - Implement SQL query execution
    - Implement time range filtering

20. **Step 3.2**: Implement `QueryEndpoint`
    - Create `QueryEndpoint.cs`
    - Map GET/POST `/v1/query`
    - Handle query parameters

21. **Step 3.3**: Set up gRPC/Protobuf
    - Add OTLP protobuf definitions
    - Configure gRPC services in Program.cs

22. **Step 3.4**: Implement OTLP ingestion
    - Create `OtlpNormalizer.cs`
    - Create `OtlpIngestionEndpoint.cs`
    - Map gRPC `/opentelemetry.proto.collector.logs.v1.LogsService/Export`

23. **Step 3.5**: Add unit tests for Phase 3
    - `QueryServiceTests.cs`
    - `OtlpNormalizerTests.cs`
    - Integration tests for query pipeline

### Phase 4: Hardening

24. **Step 4.1**: Implement L2 Compaction
    - Create `L2Compactor.cs`
    - Consolidate daily/weekly Parquet files
    - Apply ZSTD compression

25. **Step 4.2**: Implement observability
    - Create `LuminaMetrics.cs`
    - Add `wal_corruptions_detected` counter
    - Add `ingestion_rate_bytes` rate
    - Add `compaction_lag_ms` gauge
    - Expose `/metrics` endpoint

26. **Step 4.3**: Implement health checks
    - Create `HealthChecks.cs`
    - Add `/health` endpoint
    - Check WAL directory accessibility
    - Check Parquet directory accessibility

27. **Step 4.4**: Create fuzz tests
    - Create `WalFuzzTests.cs`
    - Test with intentional random bit flips
    - Test with truncated files
    - Verify recovery behavior

28. **Step 4.5**: Add OpenAPI/Swagger
    - Configure Swagger generation
    - Add API documentation
    - Expose `/swagger` endpoint

29. **Step 4.6**: Final integration tests
    - `EndToEndTests.cs`
    - `ConcurrentIngestionTests.cs`
    - `CompactionIntegrationTests.cs`

30. **Step 4.7**: Performance validation
    - Benchmark ingestion throughput
    - Verify 500 MB/s target
    - Profile memory allocations