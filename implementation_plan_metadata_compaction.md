# Implementation Plan: Metadata-Driven Compaction & System Fields

Refactor Lumina's compaction system to use metadata-driven file tracking with system field standardization (`timestamp` → `_t`), eliminating fragile filename parsing and enabling efficient time-range queries through Parquet statistics extraction.

The current implementation relies on filename parsing to extract stream names and dates, which is fragile and limits flexibility. This plan introduces a metadata-driven approach where the `CatalogEntry` stores exact `MinTime`/`MaxTime` bounds extracted from Parquet file statistics. This enables O(1) file discovery for time-range queries, eliminates filename dependencies, and supports N-tier compaction strategies. The system field `_t` replaces `timestamp` to clearly identify it as a reserved system column.

[Types]

New and modified type definitions for metadata-driven compaction.

### CatalogEntry Modifications (Lumina/Storage/Catalog/CatalogEntry.cs)
```csharp
public sealed class CatalogEntry
{
  [JsonPropertyName("streamName")]
  public required string StreamName { get; init; }

  // REMOVED: Single Date field
  // [JsonPropertyName("date")]
  // public DateTime Date { get; init; }

  /// <summary>
  /// Gets the minimum timestamp in this file (from _t column statistics).
  /// </summary>
  [JsonPropertyName("minTime")]
  public DateTime MinTime { get; init; }

  /// <summary>
  /// Gets the maximum timestamp in this file (from _t column statistics).
  /// </summary>
  [JsonPropertyName("maxTime")]
  public DateTime MaxTime { get; init; }

  [JsonPropertyName("filePath")]
  public required string FilePath { get; init; }

  [JsonPropertyName("level")]
  public StorageLevel Level { get; init; }

  [JsonPropertyName("rowCount")]
  public long RowCount { get; init; }

  [JsonPropertyName("fileSizeBytes")]
  public long FileSizeBytes { get; init; }

  [JsonPropertyName("addedAt")]
  public DateTime AddedAt { get; init; }

  /// <summary>
  /// Gets the compaction tier (for future L3+ support).
  /// </summary>
  [JsonPropertyName("compactionTier")]
  public int CompactionTier { get; init; } = 1;
}
```

### TimeRangeQueryResult (New Type)
```csharp
namespace Lumina.Storage.Catalog;

/// <summary>
/// Result of a time-range catalog query.
/// </summary>
public sealed class TimeRangeQueryResult
{
  public required IReadOnlyList<CatalogEntry> Entries { get; init; }
  public int TotalFiles { get; init; }
  public long TotalRows { get; init; }
  public DateTime? GlobalMinTime { get; init; }
  public DateTime? GlobalMaxTime { get; init; }
}
```

### LogEntry Modifications (Lumina/Core/Models/LogEntry.cs)
- Property `Timestamp` remains unchanged in the domain model for backward compatibility
- During serialization to Parquet, `Timestamp` maps to column `_t`
- During deserialization from Parquet, column `_t` maps back to `Timestamp`

### SchemaResolver Updates (Lumina/Storage/Parquet/SchemaResolver.cs)
- Fixed column `"timestamp"` changes to `"_t"`
- All references to the timestamp column use the new system field name

[Files]

File modifications organized by phase.

### Phase 1 Files - System Field Standardization

**Modified Files:**
- `Lumina/Storage/Parquet/SchemaResolver.cs`
  - Change fixed column name from `"timestamp"` to `"_t"`
  - Update `IsFixedColumn()` to recognize `"_t"`

- `Lumina/Storage/Parquet/ParquetWriter.cs`
  - Change column name from `"timestamp"` to `"_t"` in `CollectFieldDataPairs()`
  - Update `DateTimeDataField` construction

- `Lumina/Storage/Parquet/ParquetReader.cs`
  - Update `FixedColumns` set: `"timestamp"` → `"_t"`
  - Update `GetDateTime()` call to read from `"_t"` column

- `Lumina/Query/DuckDbQueryService.cs`
  - Update SQL queries to use `_t` instead of `timestamp`
  - Update `QueryLogsAsync()`, `SearchLogsAsync()`, `GetStatsAsync()`

- `Tests/Storage/SchemaResolverTests.cs`
  - Update tests to expect `"_t"` column name

- `Tests/Storage/ParquetRoundTripTests.cs`
  - Update tests for new column name

### Phase 2 Files - CatalogEntry Metadata

**Modified Files:**
- `Lumina/Storage/Catalog/CatalogEntry.cs`
  - Remove `Date` property
  - Add `MinTime` and `MaxTime` properties
  - Add `CompactionTier` property

- `Lumina/Storage/Catalog/CatalogManager.cs`
  - Add `GetFilesInRange()` method for time-range queries
  - Update `AddFileAsync()` to require `MinTime`/`MaxTime`
  - Add `GetEntriesByTimeRange()` method

### Phase 3 Files - CatalogRebuilder & ParquetManager

**Modified Files:**
- `Lumina/Storage/Catalog/CatalogRebuilder.cs`
  - Remove `ParseFilePath()` method entirely
  - Add `ExtractTimeBoundsFromParquet()` method
  - Update `CreateEntryFromFileAsync()` to read Parquet statistics
  - Remove date-based conflict resolution, use time-range overlap detection

- `Lumina/Query/ParquetManager.cs`
  - Update `GetFilesInRange()` to use catalog time-range query
  - Remove filename parsing logic from `GetFilesInRange()`

**New Files:**
- `Lumina/Storage/Parquet/ParquetStatisticsReader.cs`
  - New utility class to extract Min/Max statistics from Parquet files
  - Uses Parquet.Net metadata reading (no row data access)

### Phase 4 Files - N-Tier Compaction

**Modified Files:**
- `Lumina/Storage/Compaction/L1Compactor.cs`
  - Update to populate `MinTime`/`MaxTime` in catalog entries
  - Calculate bounds from entry timestamps during compaction

- `Lumina/Storage/Compaction/L2Compactor.cs`
  - Remove filename parsing (`ParseFileInfo()`)
  - Query catalog for files by time range
  - Implement daily compaction: L1 files where `MaxTime < UtcNow.Date`
  - Implement monthly compaction: L2 files within completed month

- `Tests/Storage/Catalog/CatalogRebuilderTests.cs`
  - Remove filename parsing tests
  - Add statistics extraction tests
  - Add time-range query tests

- `Tests/Storage/CompactionIntegrationTests.cs`
  - Add tests for metadata-driven compaction
  - Add tests for catalog rebuild with statistics extraction

[Functions]

Function-level changes across all phases.

### New Functions

**ParquetStatisticsReader.cs:**
```csharp
namespace Lumina.Storage.Parquet;

public static class ParquetStatisticsReader
{
  /// <summary>
  /// Extracts Min/Max statistics for the _t column from a Parquet file.
  /// </summary>
  /// <param name="filePath">Path to the Parquet file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Tuple of (MinTime, MaxTime) or null if unavailable.</returns>
  public static Task<(DateTime MinTime, DateTime MaxTime)?> ExtractTimeBoundsAsync(
    string filePath, 
    CancellationToken cancellationToken = default);
}
```

**CatalogManager.cs:**
```csharp
/// <summary>
/// Gets files that overlap with the specified time range.
/// </summary>
public IReadOnlyList<CatalogEntry> GetFilesInRange(
  string stream, 
  DateTime start, 
  DateTime end);

/// <summary>
/// Gets all entries within a time range across all streams.
/// </summary>
public IReadOnlyList<CatalogEntry> GetEntriesByTimeRange(
  DateTime start, 
  DateTime end,
  StorageLevel? level = null);

/// <summary>
/// Gets entries eligible for daily L2 compaction (L1 files with MaxTime < cutoff).
/// </summary>
public IReadOnlyList<CatalogEntry> GetEligibleForDailyCompaction(
  string stream,
  DateTime cutoffDate);

/// <summary>
/// Gets entries eligible for monthly consolidation.
/// </summary>
public IReadOnlyList<CatalogEntry> GetEligibleForMonthlyCompaction(
  string stream,
  int year,
  int month);
```

### Modified Functions

**CatalogRebuilder.cs:**
- `CreateEntryFromFileAsync()` - Remove `ParseFilePath()` call, add `ExtractTimeBoundsFromParquet()` call
- `ResolveConflicts()` - Change from `(StreamName, Date)` grouping to time-range overlap detection
- `ScanL1FilesAsync()` - Use statistics extraction instead of filename parsing
- `ScanL2FilesAsync()` - Use statistics extraction instead of filename parsing

**ParquetManager.cs:**
- `GetFilesInRange()` - Change to use `CatalogManager.GetFilesInRange()` instead of filename parsing

**L2Compactor.cs:**
- `CompactAllAsync()` - Use catalog queries instead of filename parsing
- `ParseFileInfo()` - **REMOVE ENTIRELY**

**DuckDbQueryService.cs:**
- `QueryLogsAsync()` - Change `timestamp` references to `_t`
- `SearchLogsAsync()` - Change `timestamp` references to `_t`
- `GetStatsAsync()` - Change `timestamp` references to `_t`

[Classes]

Class-level modifications.

### Modified Classes

**CatalogEntry (Lumina/Storage/Catalog/CatalogEntry.cs):**
- Remove: `Date` property
- Add: `MinTime` (DateTime), `MaxTime` (DateTime), `CompactionTier` (int)

**CatalogRebuilder (Lumina/Storage/Catalog/CatalogRebuilder.cs):**
- Remove: `ParsedFileInfo` inner class
- Remove: `ParseFilePath()` method
- Add: Time-range based conflict resolution
- Add: Parquet statistics extraction integration

**L2Compactor (Lumina/Storage/Compaction/L2Compactor.cs):**
- Remove: `ParsedFileInfo` inner class
- Remove: `ParseFileInfo()` method
- Modify: Use catalog time-range queries for file discovery

### New Classes

**ParquetStatisticsReader (Lumina/Storage/Parquet/ParquetStatisticsReader.cs):**
- Static utility class
- Method: `ExtractTimeBoundsAsync()`
- Uses Parquet.Net row group statistics for O(1) metadata access

**TimeRangeQueryResult (Lumina/Storage/Catalog/TimeRangeQueryResult.cs):**
- Result container for time-range queries
- Properties: `Entries`, `TotalFiles`, `TotalRows`, `GlobalMinTime`, `GlobalMaxTime`

[Dependencies]

No new external dependencies required.

The implementation uses the existing `Parquet.Net` library which already supports reading column statistics from row group metadata. The `ParquetReader` class provides access to `RowGroup` statistics without reading actual row data.

Key Parquet.Net APIs used:
- `ParquetReader.Schema` - Get column schema
- `RowGroupReader` - Access row group metadata
- Column statistics are available in the Parquet file footer

[Testing]

Test strategy for validating metadata-driven compaction.

### Phase 1 Tests - System Field Standardization

**Update Existing Tests:**
- `Tests/Storage/SchemaResolverTests.cs` - Update to expect `"_t"` column
- `Tests/Storage/ParquetRoundTripTests.cs` - Verify `_t` column read/write

**New Tests:**
- `TimestampColumnRenamedToSystemField()` - Verify column is named `_t` in output file
- `QueryWithSystemField()` - Verify SQL queries work with `_t`

### Phase 2 Tests - CatalogEntry Metadata

**New Tests in CatalogManagerTests.cs:**
```csharp
[Fact] void AddFile_RequiresMinTimeMaxTime()
[Fact] void GetFilesInRange_ReturnsOverlappingFiles()
[Fact] void GetFilesInRange_ExcludesNonOverlappingFiles()
[Fact] void GetEntriesByTimeRange_FiltersByLevel()
```

### Phase 3 Tests - Statistics Extraction

**New Tests in CatalogRebuilderTests.cs:**
```csharp
[Fact] async Task Rebuild_ExtractsTimeBoundsFromParquet()
[Fact] async Task Rebuild_HandlesMissingStatistics()
[Fact] async Task ResolveConflicts_DetectsTimeRangeOverlap()
```

**New Tests for ParquetStatisticsReader:**
```csharp
[Fact] async Task ExtractTimeBounds_ReturnsMinMaxFromStatistics()
[Fact] async Task ExtractTimeBounds_ReturnsNull_WhenColumnMissing()
[Fact] async Task ExtractTimeBounds_HandlesEmptyFile()
```

### Phase 4 Tests - N-Tier Compaction

**Update Tests in CompactionIntegrationTests.cs:**
```csharp
[Fact] async Task L1Compactor_PopulatesTimeBounds()
[Fact] async Task L2Compactor_UsesCatalogQueries()
[Fact] async Task L2Compactor_CreatesDailyFiles()
[Fact] async Task L2Compactor_CreatesMonthlyFiles()
[Fact] async Task CatalogRebuild_RestoresQueryRouting()
```

### Verification Test Scenario

1. Ingest test data spanning multiple days
2. Run L1 compaction
3. Run L2 compaction
4. Delete `catalog.json`
5. Restart service (trigger rebuild)
6. Verify catalog entries have correct `MinTime`/`MaxTime`
7. Execute time-range query
8. Verify correct files are selected

[Implementation Order]

Sequential implementation steps to minimize conflicts.

1. **Phase 1: System Field Standardization**
   - Update `SchemaResolver.cs` - change `"timestamp"` to `"_t"`
   - Update `ParquetWriter.cs` - write to `"_t"` column
   - Update `ParquetReader.cs` - read from `"_t"` column
   - Update `DuckDbQueryService.cs` - SQL queries use `_t`
   - Run and fix existing tests

2. **Phase 2: CatalogEntry Metadata**
   - Update `CatalogEntry.cs` - add `MinTime`, `MaxTime`, `CompactionTier`; remove `Date`
   - Update `CatalogManager.cs` - add time-range query methods
   - Add `TimeRangeQueryResult.cs`
   - Write unit tests for new catalog methods

3. **Phase 3: Statistics Extraction**
   - Create `ParquetStatisticsReader.cs`
   - Update `CatalogRebuilder.cs` - use statistics instead of filename parsing
   - Update `ParquetManager.cs` - use catalog time-range queries
   - Write tests for statistics extraction
   - Write tests for catalog rebuild

4. **Phase 4: N-Tier Compaction**
   - Update `L1Compactor.cs` - populate time bounds
   - Update `L2Compactor.cs` - remove filename parsing, use catalog queries
   - Implement daily compaction logic
   - Implement monthly compaction logic
   - Write integration tests
   - Run full test suite

5. **Final Verification**
   - Run all tests
   - Manual end-to-end verification
   - Performance validation of time-range queries