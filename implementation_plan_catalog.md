# Implementation Plan: Stream Catalog for Atomic Visibility

[Overview]
Implement a Stream Catalog system that serves as the single source-of-truth for active data files in Lumina, eliminating read-during-write corruption, preventing duplicate row visibility during L2 compaction, and providing ACID-like atomic transitions.

The Stream Catalog introduces `catalog.json` as an authoritative registry of all active Parquet files. This solves three critical problems: (1) Queries currently use `Directory.GetFiles()` which can observe partial writes or inconsistent file sets during compaction; (2) During L2 compaction, both L1 and L2 files for the same Stream+Date are briefly visible, causing duplicate rows; (3) There is no atomic transition point that guarantees consistent query results. The catalog provides a single atomic commit point where file visibility changes take effect.

[Types]
Define new data models in the `Lumina/Storage/Catalog/` directory for representing catalog entries and state.

**CatalogEntry.cs**
```csharp
namespace Lumina.Storage.Catalog;

/// <summary>
/// Represents an active Parquet file in the stream catalog.
/// </summary>
public sealed class CatalogEntry
{
    /// <summary>
    /// Gets the stream name this file belongs to.
    /// </summary>
    public required string StreamName { get; init; }
    
    /// <summary>
    /// Gets the date (UTC) this file covers. Used for L2 compaction grouping.
    /// </summary>
    public DateTime Date { get; init; }
    
    /// <summary>
    /// Gets the absolute file path.
    /// </summary>
    public required string FilePath { get; init; }
    
    /// <summary>
    /// Gets the storage level: L1 (uncompacted) or L2 (daily consolidated).
    /// </summary>
    public StorageLevel Level { get; init; }
    
    /// <summary>
    /// Gets the number of log entries in this file.
    /// </summary>
    public long RowCount { get; init; }
    
    /// <summary>
    /// Gets the file size in bytes.
    /// </summary>
    public long FileSizeBytes { get; init; }
    
    /// <summary>
    /// Gets the timestamp when this entry was added to the catalog.
    /// </summary>
    public DateTime AddedAt { get; init; }
}

/// <summary>
/// Storage level for catalog entries.
/// </summary>
public enum StorageLevel
{
    /// <summary>
    /// L1: Uncompacted Parquet files from WAL conversion.
    /// </summary>
    L1 = 1,
    
    /// <summary>
    /// L2: Daily consolidated Parquet files.
    /// </summary>
    L2 = 2
}
```

**StreamCatalog.cs**
```csharp
namespace Lumina.Storage.Catalog;

/// <summary>
/// Represents the complete catalog state.
/// </summary>
public sealed class StreamCatalog
{
    /// <summary>
    /// Gets or sets the list of catalog entries.
    /// </summary>
    public List<CatalogEntry> Entries { get; set; } = new();
    
    /// <summary>
    /// Gets or sets the last modification timestamp.
    /// </summary>
    public DateTime LastModified { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Gets or sets the catalog version for optimistic concurrency.
    /// </summary>
    public long Version { get; set; } = 1;
}
```

**CatalogOptions.cs**
```csharp
namespace Lumina.Storage.Catalog;

/// <summary>
/// Configuration options for the catalog system.
/// </summary>
public sealed class CatalogOptions
{
    /// <summary>
    /// Gets the directory where catalog.json is stored.
    /// Default is "data/catalog".
    /// </summary>
    public string CatalogDirectory { get; init; } = "data/catalog";
    
    /// <summary>
    /// Gets a value indicating whether to rebuild catalog on startup if corrupted.
    /// Default is true.
    /// </summary>
    public bool EnableAutoRebuild { get; init; } = true;
    
    /// <summary>
    /// Gets a value indicating whether to run garbage collection on startup.
    /// Default is true.
    /// </summary>
    public bool EnableStartupGc { get; init; } = true;
}
```

[Files]
Create new files and modify existing files to integrate the Stream Catalog system.

**New Files to Create:**

1. `Lumina/Storage/Catalog/CatalogEntry.cs` - Model for individual catalog entries
2. `Lumina/Storage/Catalog/StreamCatalog.cs` - Model for complete catalog state
3. `Lumina/Storage/Catalog/StorageLevel.cs` - Enum for L1/L2 levels (can be in CatalogEntry.cs)
4. `Lumina/Storage/Catalog/CatalogOptions.cs` - Configuration options
5. `Lumina/Storage/Catalog/CatalogManager.cs` - Core catalog management with safe-write pattern
6. `Lumina/Storage/Catalog/CatalogRebuilder.cs` - Disaster recovery from disk scan
7. `Lumina/Storage/Catalog/CatalogGarbageCollector.cs` - Startup GC for orphaned files
8. `Tests/Storage/Catalog/CatalogManagerTests.cs` - Unit tests for catalog operations
9. `Tests/Storage/Catalog/CatalogRebuilderTests.cs` - Tests for disaster recovery
10. `Tests/Storage/Catalog/CatalogConcurrencyTests.cs` - Concurrency and atomic visibility tests

**Existing Files to Modify:**

1. `Lumina/Core/Configuration/CompactionSettings.cs`
   - Add `CatalogDirectory` property (default: "data/catalog")
   - Add `EnableCatalogAutoRebuild` property (default: true)
   - Add `EnableCatalogStartupGc` property (default: true)

2. `Lumina/Query/ParquetManager.cs`
   - Add `CatalogManager` dependency injection
   - Refactor `DiscoverAllStreams()` to query catalog instead of filesystem
   - Refactor `GetStreamFiles()` to use catalog entries
   - Refactor `GetL1Files()` to use catalog entries
   - Refactor `GetL2Files()` to use catalog entries
   - Refactor `GetTotalSize()` to use catalog entries
   - Keep filesystem scanning as fallback for recovery scenarios

3. `Lumina/Storage/Compaction/L1Compactor.cs`
   - Inject `CatalogManager` dependency
   - Call `CatalogManager.AddFileAsync()` after Parquet file is written
   - Pass row count to catalog entry

4. `Lumina/Storage/Compaction/L2Compactor.cs`
   - Inject `CatalogManager` dependency
   - Implement atomic commit: write L2 file → call `CatalogManager.ReplaceFilesAsync(oldL1Files, newL2File)` → delete L1 files
   - Ensure catalog update happens before physical deletion

5. `Lumina/Program.cs`
   - Register `CatalogManager` as singleton service
   - Initialize catalog on startup
   - Run startup GC after catalog initialization
   - Update service registration order for proper dependency injection

6. `Lumina/Storage/Compaction/CompactorService.cs`
   - Optionally trigger catalog refresh if needed

[Functions]
Define new functions and modifications to existing functions.

**New Functions:**

1. `CatalogManager.cs`:
   - `Task InitializeAsync(CancellationToken)` - Load or rebuild catalog on startup
   - `Task AddFileAsync(CatalogEntry, CancellationToken)` - Add new L1 file after write
   - `Task ReplaceFilesAsync(IReadOnlyList<string> oldFiles, CatalogEntry newFile, CancellationToken)` - Atomic L2 compaction commit
   - `Task RemoveFileAsync(string filePath, CancellationToken)` - Remove file from catalog
   - `IReadOnlyList<CatalogEntry> GetEntries(string? stream = null, StorageLevel? level = null)` - Query entries
   - `IReadOnlyList<string> GetStreams()` - Get all stream names
   - `IReadOnlyList<string> GetFiles(string stream)` - Get files for a stream
   - `Task PersistAsync(CancellationToken)` - Safe-write to disk
   - `void ReloadFromState(StreamCatalog)` - Update in-memory state

2. `CatalogRebuilder.cs`:
   - `Task<StreamCatalog> RecoverFromDiskAsync(string l1Dir, string l2Dir, CancellationToken)` - Rebuild from disk
   - `IEnumerable<CatalogEntry> ScanL1Files(string l1Dir)` - Scan L1 directory
   - `IEnumerable<CatalogEntry> ScanL2Files(string l2Dir)` - Scan L2 directory
   - `StreamCatalog ResolveConflicts(IEnumerable<CatalogEntry>)` - L2 priority conflict resolution

3. `CatalogGarbageCollector.cs`:
   - `Task RunGcAsync(StreamCatalog catalog, string l1Dir, string l2Dir, CancellationToken)` - Delete orphaned files
   - `IReadOnlyList<string> FindOrphanedFiles(StreamCatalog, string l1Dir, string l2Dir)` - Find unreferenced files

**Modified Functions:**

1. `ParquetManager.DiscoverAllStreams()`:
   - Current: Uses `Directory.GetDirectories()` on L1/L2 paths
   - Modified: Uses `_catalogManager.GetStreams()` from in-memory catalog

2. `ParquetManager.GetStreamFiles(string stream)`:
   - Current: Uses `Directory.GetFiles()` on L1/L2 stream directories
   - Modified: Uses `_catalogManager.GetFiles(stream)` from catalog

3. `ParquetManager.GetL1Files()`:
   - Current: Scans L1 directory recursively
   - Modified: Queries catalog entries with `StorageLevel.L1`

4. `ParquetManager.GetL2Files()`:
   - Current: Scans L2 directory recursively
   - Modified: Queries catalog entries with `StorageLevel.L2`

5. `ParquetManager.GetTotalSize()`:
   - Current: Sums file sizes from filesystem
   - Modified: Sums `FileSizeBytes` from catalog entries

6. `L1Compactor.CompactStreamAsync(string, CancellationToken)`:
   - After: `ParquetWriter.WriteBatchAsync(...)` completes successfully
   - Add: `await _catalogManager.AddFileAsync(new CatalogEntry { ... })`

7. `L2Compactor.ConsolidateDayAsync(...)`:
   - After: `ParquetWriter.WriteBatchAsync(...)` for L2 file
   - Add: `await _catalogManager.ReplaceFilesAsync(l1Files, l2Entry)`
   - Then: Delete physical L1 files

[Classes]
Define new classes and modifications to existing classes.

**New Classes:**

1. **CatalogManager** (`Lumina/Storage/Catalog/CatalogManager.cs`)
   - Purpose: Manage in-memory catalog state and persist safely to disk
   - Key Methods: InitializeAsync, AddFileAsync, ReplaceFilesAsync, PersistAsync
   - Dependencies: CatalogOptions, ILogger<CatalogManager>
   - Thread Safety: Uses lock or SemaphoreSlim for concurrent access
   - Safe-Write Pattern:
     ```csharp
     async Task SafeWriteAsync(StreamCatalog state, CancellationToken ct)
     {
         var tmpPath = Path.Combine(_options.CatalogDirectory, "catalog.tmp.json");
         var finalPath = Path.Combine(_options.CatalogDirectory, "catalog.json");
         
         await using var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None);
         await JsonSerializer.SerializeAsync(stream, state, _jsonOptions, ct);
         await stream.FlushAsync(ct);
         
         // Atomic rename
         File.Move(tmpPath, finalPath, overwrite: true);
     }
     ```

2. **CatalogRebuilder** (`Lumina/Storage/Catalog/CatalogRebuilder.cs`)
   - Purpose: Rebuild catalog from physical disk when catalog.json is missing/corrupted
   - Key Methods: RecoverFromDiskAsync, ScanL1Files, ScanL2Files
   - Conflict Resolution Logic:
     - Group entries by (StreamName, Date)
     - If L2 exists for group: use L2, ignore L1 files
     - If no L2: include all L1 files

3. **CatalogGarbageCollector** (`Lumina/Storage/Catalog/CatalogGarbageCollector.cs`)
   - Purpose: Clean up orphaned files on startup
   - Key Methods: RunGcAsync, FindOrphanedFiles
   - Cleanup Targets: Unreferenced .parquet files, .tmp files

**Modified Classes:**

1. **ParquetManager** (`Lumina/Query/ParquetManager.cs`)
   - Add: `CatalogManager _catalogManager` field
   - Modify constructor to accept CatalogManager
   - Add fallback methods for filesystem scanning (used by CatalogRebuilder only)

2. **L1Compactor** (`Lumina/Storage/Compaction/L1Compactor.cs`)
   - Add: `CatalogManager _catalogManager` field
   - Modify constructor to accept CatalogManager
   - Modify CompactStreamAsync to register new files in catalog

3. **L2Compactor** (`Lumina/Storage/Compaction/L2Compactor.cs`)
   - Add: `CatalogManager _catalogManager` field
   - Modify constructor to accept CatalogManager
   - Modify ConsolidateDayAsync for atomic commit pattern

4. **CompactionSettings** (`Lumina/Core/Configuration/CompactionSettings.cs`)
   - Add catalog-related configuration properties

[Dependencies]
No new external NuGet packages required.

The implementation uses existing dependencies:
- `System.Text.Json` for catalog serialization (built-in .NET 8)
- `Microsoft.Extensions.Logging` for logging (already used)
- `Parquet.Net` for Parquet file operations (already used)

Add project reference if creating test project separately (not needed if tests remain in existing `Lumina.Tests` project).

[Testing]
Comprehensive testing strategy for catalog functionality.

**Unit Tests:**

1. **CatalogManagerTests.cs**
   - `AddFileAsync_ShouldAddEntryAndPersist` - Verify entry added and persisted
   - `ReplaceFilesAsync_ShouldAtomicallySwapFiles` - Verify atomic replacement
   - `PersistAsync_ShouldUseSafeWritePattern` - Verify tmp → rename pattern
   - `PersistAsync_WhenExceptionDuringMove_OriginalCatalogPreserved` - Safe-write test
   - `GetStreams_ShouldReturnUniqueStreamNames` - Stream discovery
   - `GetFiles_ShouldReturnFilesForStream` - File query

2. **CatalogRebuilderTests.cs**
   - `RecoverFromDiskAsync_ShouldScanBothLevels` - Directory scanning
   - `ResolveConflicts_WhenL2Exists_ShouldPreferL2` - Conflict resolution
   - `RecoverFromDiskAsync_WithCorruptedCatalog_ShouldRebuild` - Recovery scenario

3. **CatalogConcurrencyTests.cs**
   - `ConcurrentReadsDuringWrite_ShouldNotSeePartialState` - Atomic visibility
   - `L2Compaction_ShouldNotCauseDuplicateRows` - Row duplication prevention
   - `MultipleCompactors_ShouldNotRace` - Concurrent compaction safety

**Integration Tests:**

1. **CatalogIntegrationTests.cs** (in `Tests/Storage/`)
   - End-to-end test: Write WAL → Compact L1 → Verify catalog
   - End-to-end test: Compact L2 → Verify atomic visibility
   - Recovery test: Delete catalog.json → Restart → Verify rebuild

**Test File Modifications:**

1. `Tests/Storage/CompactionIntegrationTests.cs`
   - Update to use CatalogManager
   - Add catalog verification assertions

[Implementation Order]
Sequential implementation steps to minimize conflicts and ensure successful integration.

1. **Create Catalog Models** (no dependencies)
   - Create `Lumina/Storage/Catalog/` directory
   - Create `CatalogEntry.cs` with StorageLevel enum
   - Create `StreamCatalog.cs`
   - Create `CatalogOptions.cs`

2. **Implement CatalogManager Core** (depends on models)
   - Create `CatalogManager.cs` with in-memory state management
   - Implement `GetStreams()`, `GetFiles()`, `GetEntries()`
   - Implement safe-write pattern in `PersistAsync()`
   - Implement `AddFileAsync()` and `RemoveFileAsync()`

3. **Implement CatalogRebuilder** (depends on models)
   - Create `CatalogRebuilder.cs`
   - Implement L1/L2 directory scanning
   - Implement conflict resolution logic
   - Integrate with CatalogManager initialization

4. **Implement CatalogGarbageCollector** (depends on CatalogManager)
   - Create `CatalogGarbageCollector.cs`
   - Implement orphan file detection
   - Implement safe deletion with logging

5. **Update Configuration** (no dependencies)
   - Modify `CompactionSettings.cs` to add catalog options

6. **Refactor L1Compactor** (depends on CatalogManager)
   - Add CatalogManager dependency injection
   - Modify `CompactStreamAsync()` to register new files
   - Test L1 compaction integration

7. **Refactor L2Compactor** (depends on CatalogManager)
   - Add CatalogManager dependency injection
   - Modify `ConsolidateDayAsync()` for atomic commit
   - Test L2 compaction with atomic visibility

8. **Refactor ParquetManager** (depends on CatalogManager)
   - Add CatalogManager dependency injection
   - Modify all file discovery methods to use catalog
   - Keep filesystem methods as internal for rebuilder use

9. **Update Program.cs** (depends on all above)
   - Register CatalogManager as singleton
   - Initialize catalog on startup
   - Run GC after initialization
   - Update DI registrations for modified classes

10. **Create Unit Tests**
    - Create `Tests/Storage/Catalog/` directory
    - Implement CatalogManagerTests.cs
    - Implement CatalogRebuilderTests.cs
    - Implement CatalogConcurrencyTests.cs

11. **Update Integration Tests**
    - Update CompactionIntegrationTests.cs
    - Add catalog-specific integration tests

12. **Final Verification**
    - Run all tests
    - Verify catalog.json is created on startup
    - Verify L1 compaction updates catalog
    - Verify L2 compaction atomic commit
    - Verify recovery from deleted catalog