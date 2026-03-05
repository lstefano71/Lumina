# Implementation Plan: Race Condition Review

This document provides a comprehensive analysis of potential race conditions identified in the Lumina codebase, along with severity ratings and recommended fixes.

[Overview]
A thorough review of the Lumina log storage engine identified multiple potential race conditions across concurrent ingestion, compaction, and query execution paths. While the codebase demonstrates strong concurrency fundamentals with proper locking in many areas, several subtle issues were discovered that could lead to data corruption, lost updates, or inconsistent query results under high concurrent load.

[Types]
No new type definitions required. This is an analysis and remediation task.

[Files]
The following files require analysis and potential modification:

**High Priority (Potential Data Loss/Corruption):**
- `Lumina/Storage/Wal/WalWriter.cs` - Offset calculation and file position synchronization
- `Lumina/Storage/Compaction/L1Compactor.cs` - WAL read during active ingestion window
- `Lumina/Storage/Wal/WalHotBuffer.cs` - Version tracking TOCTOU issues
- `Lumina/Query/LiveQueryRefreshService.cs` - Version check to snapshot gap

**Medium Priority (Inconsistency Risk):**
- `Lumina/Query/DuckDbQueryService.cs` - Multiple lock coordination
- `Lumina/Storage/Compaction/CompactorService.cs` - Non-thread-safe field access
- `Lumina/Storage/Wal/WalManager.cs` - Rotated writer disposal timing

**Low Priority (Best Practice Improvements):**
- `Lumina/Storage/Catalog/CatalogManager.cs` - Async lock in sync methods
- `Lumina/Query/ParquetManager.cs` - Stale catalog reads

[Functions]
The following functions have race condition risks:

**CRITICAL: WalWriter.WriteAsync (Lumina/Storage/Wal/WalWriter.cs:86-115)**
```
Issue: The _currentOffset is updated with Interlocked.Add, then RandomAccess.WriteAsync 
writes at that offset. If the write fails or is cancelled after the offset reservation,
the reserved space remains as a gap in the file, potentially causing integrity issues.

Risk Level: HIGH
Scenario: Thread A reserves offset 1000, write fails. Thread B reserves offset 1500.
Result: Gap in file from 1000-1500 containing garbage data.
```

**CRITICAL: L1Compactor.CompactStreamAsync (Lumina/Storage/Compaction/L1Compactor.cs:52-140)**
```
Issue: WAL entries are read into memory, then cursor is updated AFTER Parquet write.
During this window, new entries can be written to the active WAL file. If rotation
occurs after reading but before cursor update, entries may be:
1. Written to the rotated file (deferred for deletion)
2. Not included in current compaction
3. Lost when the rotated file is deleted

Risk Level: HIGH
Scenario: 
T1: L1Compactor reads entries 1-100 from active WAL
T2: Ingestion writes entries 101-150 to active WAL  
T3: L1Compactor completes Parquet write with entries 1-100
T4: Rotation happens, moving active WAL (containing 101-150) to sealed
T5: Cursor updated to offset 100
T6: Sealed WAL deleted (entries 101-150 LOST)
```

**HIGH: LiveQueryRefreshService.RefreshChangedStreamsAsync (Lumina/Query/LiveQueryRefreshService.cs:67-88)**
```
Issue: TOCTOU (Time-Of-Check-Time-Of-Use) between GetStreamVersion and TakeSnapshot.
Version is read, compared, then snapshot is taken. New entries can be appended between
version check and snapshot, causing the version tracking to be out of sync.

Risk Level: MEDIUM-HIGH
Scenario:
T1: GetStreamVersion returns version 100
T2: New entries appended, version becomes 101
T3: TakeSnapshot captures entries including new ones
T4: _lastSeenVersions[stream] = 100 (should be 101)
Result: Next refresh cycle won't detect changes since version 101
```

**HIGH: WalHotBuffer.Version and StreamBuffer.Version coordination (Lumina/Storage/Wal/WalHotBuffer.cs)**
```
Issue: Global version incremented AFTER stream-specific operation completes.
Between Append completing and Interlocked.Increment(ref _version), readers checking
Version may see stale global version while stream buffer has new data.

Risk Level: MEDIUM
Scenario:
T1: buffer.Append(entry) completes (stream version incremented)
T2: Reader checks global Version - still old value
T3: Interlocked.Increment(ref _version) happens
Result: Brief window where stream has new data but global version unchanged
```

**MEDIUM: DuckDbQueryService lock coordination (Lumina/Query/DuckDbQueryService.cs)**
```
Issue: Multiple synchronization primitives used:
- _dbLock (SemaphoreSlim) for DuckDB connection
- _streamLockManager.CompactionLock for file visibility
- _registrationLock (object) for _registeredStreams HashSet
- ConcurrentDictionary for _hotTableStreams and _hotTableSchemas

The CompactionLock reader lock is held during query execution, but view rebuilds
and hot table operations happen under _dbLock with no coordination to prevent
schema changes mid-query.

Risk Level: MEDIUM
```

**MEDIUM: CompactorService._lastL2Run (Lumina/Storage/Compaction/CompactorService.cs:23)**
```
Issue: DateTime _lastL2Run = DateTime.MinValue is a non-atomic field accessed
from the background service loop. While currently only one loop iteration runs
at a time, the field is not protected against concurrent reads.

Risk Level: LOW-MEDIUM (would only matter if ExecuteAsync is called concurrently)
```

**MEDIUM: WalManager.ForceRotateAsync disposal timing (Lumina/Storage/Wal/WalWriter.cs:89-118)**
```
Issue: In ForceRotateAsync, the old writer is disposed immediately after swap.
In RotateWalIfNeededAsync, old writer is added to _rotatedWriters for deferred
disposal. In-flight writes to the old writer after disposal could fail.

The code does flush before disposal, but concurrent WriteAsync calls could
be in progress.

Risk Level: MEDIUM
```

**LOW: CursorManager vs WalHotBuffer coordination (Lumina/Storage/Compaction/L1Compactor.cs:124-128)**
```
Issue: Cursor is updated BEFORE EvictCompacted is called. If process crashes
between these calls, on restart:
1. Cursor shows entries compacted
2. Hot buffer still contains those entries
Result: Double-counting of entries in queries until next compaction

This is correctly handled by WalStartupReplayService which uses cursor to
filter replay, so entries in hot buffer from before crash would be filtered.

Risk Level: LOW (handled by startup replay)
```

[Classes]
Analysis of class-level thread safety:

**AsyncReaderWriterLock (Lumina/Core/Concurrency/AsyncReaderWriterLock.cs)**
- CORRECT: LockGuard uses Interlocked.Exchange for dispose-once guarantee
- CORRECT: Delegates to DotNext which is well-tested
- Status: No issues found

**StreamLockManager (Lumina/Core/Concurrency/StreamLockManager.cs)**
- CORRECT: ConcurrentDictionary for per-stream locks
- CORRECT: CompactionLock is properly exposed
- Status: No issues found

**WalManager (Lumina/Storage/Wal/WalManager.cs)**
- CORRECT: Double-checked locking in GetOrCreateWriterAsync
- CORRECT: Per-stream SemaphoreSlim for rotation serialization
- ISSUE: DisposeAsync iterates and disposes writers without preventing new access
- Status: Minor issue in disposal path

**WalWriter (Lumina/Storage/Wal/WalWriter.cs)**
- CORRECT: Interlocked.Add for offset reservation
- CORRECT: RandomAccess.WriteAsync for concurrent writes
- ISSUE: No rollback for failed writes after offset reservation
- Status: Potential file corruption on write failure

**WalHotBuffer (Lumina/Storage/Wal/WalHotBuffer.cs)**
- CORRECT: Per-stream Lock for buffer operations
- CORRECT: Snapshot returns copy of list
- ISSUE: Global version increment not atomic with stream operation
- Status: Minor TOCTOU possible

**CursorManager (Lumina/Storage/Compaction/CursorManager.cs)**
- CORRECT: Single lock object for all operations
- CORRECT: Deep copy returned from GetCursor
- CORRECT: MarkCompactionComplete holds lock for entire sequence
- Status: Properly implemented

**CatalogManager (Lumina/Storage/Catalog/CatalogManager.cs)**
- CORRECT: SemaphoreSlim for async operations
- ISSUE: GetEntries() reads _catalog.Entries without lock (stale read acceptable)
- Status: Minor inconsistency but not a bug

**DuckDbQueryService (Lumina/Query/DuckDbQueryService.cs)**
- CORRECT: Reader lock held during query
- CORRECT: Writer lock acquired for refresh+delete in compaction
- ISSUE: Multiple uncoordinated locks
- Status: Works but complex, fragile

[Dependencies]
No new dependencies required. The existing `DotNext.Threading` package provides adequate async synchronization primitives.

[Testing]
Existing tests that verify race condition handling:

1. **Tests/Storage/ConcurrentIngestionTests.cs**
   - `ConcurrentWrites_SameStream_ShouldPreserveAllEntries` ✓
   - `ConcurrentWrites_DifferentStreams_ShouldIsolateData` ✓
   - `ConcurrentWrites_WithRotation_ShouldNotLoseData` ✓ (tests rotation during writes)

2. **Tests/Query/QueryVsCompactionTests.cs**
   - `ReaderLock_PreventsWriterDeletion_WhileQueryActive` ✓
   - `MultipleReaders_DoNotBlockEachOther` ✓
   - `CompactionUnderWriterLock_ThenQuery_Succeeds` ✓
   - `WriterLock_ExcludesOtherWriters` ✓

**Recommended Additional Tests:**
1. Test: L1Compactor with concurrent ingestion during compaction
2. Test: WriteAsync failure handling (offset reservation without write)
3. Test: Hot buffer version consistency under concurrent append/snapshot
4. Test: LiveQueryRefreshService version tracking accuracy
5. Test: WAL rotation timing edge cases

[Implementation Order]
The following order minimizes risk and ensures proper testing at each stage:

1. **Phase 1: Critical Data Loss Prevention (HIGH PRIORITY)**
   - [ ] Fix L1Compactor compaction window - Acquire stream reader lock during compaction
   - [ ] Add WAL read barrier to prevent reading entries being actively written
   - [ ] Ensure cursor update and hot buffer eviction are atomic

2. **Phase 2: Write Integrity (HIGH PRIORITY)**
   - [ ] Add write failure recovery in WalWriter - track reserved offsets
   - [ ] Consider file pre-allocation to avoid gaps
   - [ ] Add integrity validation on WAL read to detect gaps

3. **Phase 3: Query Consistency (MEDIUM PRIORITY)**
   - [ ] Fix LiveQueryRefreshService TOCTOU - combine version check and snapshot
   - [ ] Add version tracking synchronization in WalHotBuffer
   - [ ] Ensure hot table schema changes are atomic with view rebuilds

4. **Phase 4: Best Practices (LOW PRIORITY)**
   - [ ] Make CompactorService._lastL2Run thread-safe
   - [ ] Add proper disposal synchronization in WalManager
   - [ ] Document the lock hierarchy for DuckDbQueryService

5. **Phase 5: Testing**
   - [ ] Add stress tests for concurrent ingestion + compaction
   - [ ] Add failure injection tests for write failures
   - [ ] Add timing-sensitive tests for race conditions

---

## Detailed Issue Analysis

### Issue 1: L1Compactor Data Loss Window (CRITICAL)

**Location:** `Lumina/Storage/Compaction/L1Compactor.cs:52-140`

**Problem:**
The compaction process has a window where data can be lost:
1. WAL entries are read into memory
2. New entries can be written to active WAL during processing
3. Parquet file is written
4. WAL is rotated (if active WAL was compacted)
5. Old WAL files are deleted

If ingestion continues during step 2-5, entries written after the read but before rotation may be lost when the sealed WAL is deleted.

**Evidence from code:**
```csharp
// Line 52-140: CompactStreamAsync
var walFiles = _walManager.GetWalFiles(stream);
foreach (var walFile in walFiles) {
    // Entries read here...
}

// ... entries processed ...

// Active WAL rotation happens here
var activeFilePath = _walManager.GetActiveWriterFilePath(stream);
if (string.Equals(currentLastWalFile, activeFilePath, ...)) {
    await _walManager.ForceRotateAsync(stream, cancellationToken);
    // Window: new writes go to NEW active file
}

// Deletion of sealed WALs - entries written between read and rotation are LOST
foreach (var walFile in _walManager.GetWalFiles(stream)) {
    // Deletion logic
}
```

**Recommended Fix:**
Acquire the stream's writer lock during the read phase to prevent new writes:
```csharp
// In L1Compactor.CompactStreamAsync
await using var writerGuard = await _streamLockManager.AcquireStreamWriterAsync(stream, cancellationToken);
// ... rest of compaction ...
```

However, this blocks ingestion for the entire compaction duration. A better approach:
1. Read entries
2. Acquire writer lock
3. Read any new entries since step 1
4. Complete compaction
5. Rotate if needed
6. Release writer lock
7. Delete sealed files

### Issue 2: WalWriter Gap Creation (HIGH)

**Location:** `Lumina/Storage/Wal/WalWriter.cs:86-115`

**Problem:**
When `WriteAsync` fails after reserving offset space, the gap remains in the file. Subsequent writes fill in after the gap, creating an inconsistent file structure.

**Evidence from code:**
```csharp
var offset = Interlocked.Add(ref _currentOffset, totalSize) - totalSize;
// If write fails here, offset space is reserved but empty
await RandomAccess.WriteAsync(_handle, buffer.AsMemory(0, totalSize), offset, cancellationToken);
// No rollback of offset on failure
```

**Recommended Fix:**
Track failed writes and either:
1. Write a tombstone marker at the reserved offset
2. Maintain a "high water mark" separate from current offset
3. Add recovery logic to detect and skip gaps on read

### Issue 3: LiveQueryRefreshService TOCTOU (MEDIUM-HIGH)

**Location:** `Lumina/Query/LiveQueryRefreshService.cs:67-88`

**Problem:**
Version check and snapshot acquisition are not atomic.

**Evidence from code:**
```csharp
var currentVersion = _hotBuffer.GetStreamVersion(stream);
_lastSeenVersions.TryGetValue(stream, out var lastVersion);
if (currentVersion == lastVersion) continue; // Skip if unchanged

// GAP: New entries can be appended here
var snapshot = _hotBuffer.TakeSnapshot(stream); // Gets entries added in gap

_lastSeenVersions[stream] = currentVersion; // Stores OLD version
```

**Recommended Fix:**
Return version atomically with snapshot:
```csharp
// In WalHotBuffer, add:
public (long Version, IReadOnlyList<BufferedEntry> Snapshot) TakeSnapshotWithVersion(string stream)
{
    // Return both atomically
}
```

---

## Summary of Findings

| Issue | Severity | Likelihood | Impact | Priority |
|-------|----------|------------|--------|----------|
| L1Compactor data loss window | Critical | Medium | Data loss | 1 |
| WalWriter gap creation | High | Low | Corruption | 2 |
| LiveQueryRefresh TOCTOU | Medium-High | High | Inconsistency | 3 |
| WalHotBuffer version gap | Medium | Low | Stale data | 4 |
| DuckDbQueryService lock complexity | Medium | Low | Deadlock risk | 5 |
| CompactorService non-atomic field | Low | Very Low | Inconsistency | 6 |

The most critical issue is the L1Compactor data loss window, which could result in lost log entries under specific timing conditions during high concurrent load. This should be addressed first.