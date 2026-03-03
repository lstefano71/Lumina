# Implementation Plan

[Overview]
Improve cursor resiliency through checksum validation and cross-validation with WAL/Parquet files.

This implementation adds integrity verification to cursor files (similar to WAL frame headers) and validates cursor state against the actual filesystem to recover from corruption. The approach combines Option D (JSON with Checksum Wrapper) and Option E (Cross-Validation) to provide multi-layer protection against data loss.

[Types]

### CursorFileHeader
New structure for cursor file integrity validation.

```csharp
namespace Lumina.Storage.Compaction;

/// <summary>
/// Cursor file header with integrity validation.
/// Written at the start of each cursor file for format identification and checksum verification.
/// </summary>
public readonly struct CursorFileHeader
{
    /// <summary>
    /// Magic number for cursor file identification - "LCUR" in little-endian (4 bytes).
    /// </summary>
    public const uint ExpectedMagic = 0x52434C4C; // "LCUR" little-endian
    
    /// <summary>
    /// Current cursor format version.
    /// </summary>
    public const byte CurrentVersion = 0x01;
    
    /// <summary>
    /// Total size of the header in bytes.
    /// </summary>
    public const int Size = 16;
    
    /// <summary>
    /// Magic number for file identification.
    /// </summary>
    public readonly uint Magic;
    
    /// <summary>
    /// Format version number.
    /// </summary>
    public readonly byte Version;
    
    /// <summary>
    /// Reserved bytes for future use (3 bytes).
    /// </summary>
    public readonly byte Reserved1;
    public readonly byte Reserved2;
    public readonly byte Reserved3;
    
    /// <summary>
    /// CRC-32 checksum of the JSON payload (4 bytes).
    /// </summary>
    public readonly uint PayloadChecksum;
    
    /// <summary>
    /// Length of the JSON payload in bytes (4 bytes).
    /// </summary>
    public readonly uint PayloadLength;
}
```

### CursorValidationResult
Result type for cursor validation operations.

```csharp
namespace Lumina.Storage.Compaction;

/// <summary>
/// Result of cursor validation.
/// </summary>
public enum CursorValidationResult
{
    /// <summary>
    /// Cursor is valid and passed all checks.
    /// </summary>
    Valid,
    
    /// <summary>
    /// Cursor file not found, new cursor will be created.
    /// </summary>
    NotFound,
    
    /// <summary>
    /// Magic number mismatch - not a valid cursor file.
    /// </summary>
    InvalidMagic,
    
    /// <summary>
    /// Version mismatch - unsupported format version.
    /// </summary>
    UnsupportedVersion,
    
    /// <summary>
    /// CRC checksum mismatch - file is corrupted.
    /// </summary>
    ChecksumMismatch,
    
    /// <summary>
    /// Payload length is invalid or truncated file.
    /// </summary>
    TruncatedPayload,
    
    /// <summary>
    /// JSON deserialization failed.
    /// </summary>
    InvalidJson,
    
    /// <summary>
    /// Referenced WAL file no longer exists.
    /// </summary>
    WalFileNotFound,
    
    /// <summary>
    /// Referenced Parquet file no longer exists.
    /// </summary>
    ParquetFileNotFound
}
```

### CursorRecoveryInfo
Information about cursor recovery for logging/metrics.

```csharp
namespace Lumina.Storage.Compaction;

/// <summary>
/// Contains information about cursor recovery operations.
/// </summary>
public sealed class CursorRecoveryInfo
{
    /// <summary>
    /// The stream name.
    /// </summary>
    public required string Stream { get; init; }
    
    /// <summary>
    /// The validation result for the original cursor.
    /// </summary>
    public CursorValidationResult ValidationResult { get; init; }
    
    /// <summary>
    /// Whether recovery was performed.
    /// </summary>
    public bool WasRecovered { get; init; }
    
    /// <summary>
    /// The recovery method used, if any.
    /// </summary>
    public string? RecoveryMethod { get; init; }
    
    /// <summary>
    /// Original cursor offset before recovery.
    /// </summary>
    public long OriginalOffset { get; init; }
    
    /// <summary>
    /// Recovered cursor offset after recovery.
    /// </summary>
    public long RecoveredOffset { get; init; }
    
    /// <summary>
    /// Timestamp of the recovery operation.
    /// </summary>
    public DateTime RecoveryTimestamp { get; init; }
}
```

### CursorPayload (enhanced CompactionCursor)
Enhanced cursor model with additional metadata for validation.

```csharp
namespace Lumina.Core.Models;

/// <summary>
/// Compaction cursor tracking progress with validation metadata.
/// </summary>
public sealed class CompactionCursor
{
    /// <summary>
    /// Gets the stream name this cursor belongs to.
    /// </summary>
    public required string Stream { get; init; }
    
    /// <summary>
    /// Gets or sets the last compacted WAL file name.
    /// </summary>
    public string? LastCompactedWalFile { get; set; }
    
    /// <summary>
    /// Gets or sets the last compacted offset in the WAL file.
    /// </summary>
    public long LastCompactedOffset { get; set; }
    
    /// <summary>
    /// Gets or sets the timestamp of the last compaction.
    /// </summary>
    public DateTime LastCompactionTime { get; set; }
    
    /// <summary>
    /// Gets or sets the last Parquet file produced by compaction.
    /// </summary>
    public string? LastParquetFile { get; set; }
    
    // --- New validation fields ---
    
    /// <summary>
    /// Gets or sets the size in bytes of the last compacted WAL file.
    /// Used for sanity checking during recovery.
    /// </summary>
    public long? LastWalFileSize { get; set; }
    
    /// <summary>
    /// Gets or sets the number of entries in the last Parquet file.
    /// Used for sanity checking during recovery.
    /// </summary>
    public int? LastParquetEntryCount { get; set; }
    
    /// <summary>
    /// Gets or sets the checksum of the cursor data (computed before save).
    /// </summary>
    public uint? DataChecksum { get; set; }
}
```

[Files]

### New Files to Create

1. **Lumina/Storage/Compaction/CursorFileHeader.cs**
   - Purpose: Define the binary header structure for cursor files
   - Contains magic number, version, CRC-32 checksum, payload length

2. **Lumina/Storage/Compaction/CursorValidator.cs**
   - Purpose: Validate cursor integrity and cross-reference with filesystem
   - Methods: ValidateHeader, ValidatePayload, CrossValidateFiles

3. **Lumina/Storage/Compaction/CursorRecoveryService.cs**
   - Purpose: Handle cursor recovery from various corruption scenarios
   - Methods: TryRecoverFromCorruption, RebuildFromWalFiles, RebuildFromParquetMetadata

### Files to Modify

1. **Lumina/Storage/Compaction/CursorManager.cs**
   - Replace plain JSON serialization with header + JSON format
   - Add validation on load with recovery fallback
   - Add logging for corruption detection and recovery
   - Inject ILogger and WalManager for cross-validation

2. **Lumina/Core/Models/CompactionCursor.cs**
   - Add new validation fields (LastWalFileSize, LastParquetEntryCount)
   - Add DataChecksum property for internal validation

3. **Lumina/Storage/Compaction/L1Compactor.cs**
   - Populate new cursor fields during compaction (file sizes, entry counts)
   - Pass additional metadata to CursorManager.MarkCompactionComplete

4. **Lumina/Core/Configuration/CompactionSettings.cs**
   - Add settings for cursor validation behavior
   - Enable/disable strict validation, recovery options

### Test Files to Create

1. **Tests/Storage/CursorFileHeaderTests.cs**
   - Test header serialization/deserialization
   - Test checksum computation
   - Test validation edge cases

2. **Tests/Storage/CursorValidatorTests.cs**
   - Test validation logic
   - Test cross-validation with missing files

3. **Tests/Storage/CursorRecoveryTests.cs**
   - Test recovery from corrupted files
   - Test rebuild from WAL files
   - Test rebuild from Parquet metadata

### Test Files to Modify

1. **Tests/Storage/CursorManagerTests.cs**
   - Update tests to use new format
   - Add tests for validation on load
   - Add tests for recovery scenarios

[Functions]

### New Functions

1. **CursorFileHeader.WriteTo(Span<byte> destination)**
   - File: Lumina/Storage/Compaction/CursorFileHeader.cs
   - Purpose: Serialize header to byte span

2. **CursorFileHeader.ReadFrom(ReadOnlySpan<byte> source)**
   - File: Lumina/Storage/Compaction/CursorFileHeader.cs
   - Purpose: Deserialize header from byte span

3. **CursorFileHeader.ComputeChecksum(ReadOnlySpan<byte> payload)**
   - File: Lumina/Storage/Compaction/CursorFileHeader.cs
   - Purpose: Compute CRC-32 checksum of payload

4. **CursorValidator.ValidateAsync(string filePath, CancellationToken)**
   - File: Lumina/Storage/Compaction/CursorValidator.cs
   - Signature: `Task<(CursorValidationResult, CompactionCursor?)> ValidateAsync(string filePath, CancellationToken ct = default)`
   - Purpose: Validate cursor file integrity

5. **CursorValidator.CrossValidateFilesAsync(CompactionCursor cursor, CancellationToken)**
   - File: Lumina/Storage/Compaction/CursorValidator.cs
   - Signature: `Task<CursorValidationResult> CrossValidateFilesAsync(CompactionCursor cursor, CancellationToken ct = default)`
   - Purpose: Verify cursor references exist on filesystem

6. **CursorRecoveryService.TryRecoverAsync(string stream, CursorValidationResult error, CancellationToken)**
   - File: Lumina/Storage/Compaction/CursorRecoveryService.cs
   - Signature: `Task<(bool Success, CompactionCursor? Cursor, CursorRecoveryInfo Info)> TryRecoverAsync(string stream, CursorValidationResult error, CancellationToken ct = default)`
   - Purpose: Attempt recovery based on error type

7. **CursorRecoveryService.RebuildFromWalFilesAsync(string stream, CancellationToken)**
   - File: Lumina/Storage/Compaction/CursorRecoveryService.cs
   - Signature: `Task<CompactionCursor?> RebuildFromWalFilesAsync(string stream, CancellationToken ct = default)`
   - Purpose: Rebuild cursor by scanning WAL files

8. **CursorRecoveryService.RebuildFromParquetFilesAsync(string stream, CancellationToken)**
   - File: Lumina/Storage/Compaction/CursorRecoveryService.cs
   - Signature: `Task<CompactionCursor?> RebuildFromParquetFilesAsync(string stream, CancellationToken ct = default)`
   - Purpose: Rebuild cursor by scanning Parquet file metadata

### Modified Functions

1. **CursorManager.LoadCursors()**
   - Current: Silently ignores corrupted cursor files
   - Change: Validate header and checksum, attempt recovery, log issues
   - Add: Call CursorValidator and CursorRecoveryService

2. **CursorManager.SaveCursor(CompactionCursor cursor)**
   - Current: Write plain JSON with temp file + move
   - Change: Write header + checksum + JSON payload
   - Add: Populate DataChecksum field before save

3. **CursorManager.GetCursor(string stream)**
   - Current: Return from memory or new empty cursor
   - Change: Validate loaded cursor is still valid, trigger recovery if needed

4. **L1Compactor.CompactStreamAsync(string stream, CancellationToken)**
   - Current: Pass basic info to MarkCompactionComplete
   - Change: Populate new validation fields (LastWalFileSize, LastParquetEntryCount)

[Classes]

### New Classes

1. **CursorFileHeader** (struct)
   - File: Lumina/Storage/Compaction/CursorFileHeader.cs
   - Purpose: Binary header for cursor file integrity
   - Key methods: WriteTo, ReadFrom, ComputeChecksum, IsValid
   - Size: 16 bytes fixed

2. **CursorValidator**
   - File: Lumina/Storage/Compaction/CursorValidator.cs
   - Purpose: Validate cursor file and cross-reference
   - Dependencies: WalManager (for file existence checks)
   - Key methods: ValidateAsync, CrossValidateFilesAsync

3. **CursorRecoveryService**
   - File: Lumina/Storage/Compaction/CursorRecoveryService.cs
   - Purpose: Recover corrupted cursors
   - Dependencies: WalManager, ParquetReader, ILogger
   - Key methods: TryRecoverAsync, RebuildFromWalFilesAsync, RebuildFromParquetFilesAsync

### Modified Classes

1. **CursorManager**
   - File: Lumina/Storage/Compaction/CursorManager.cs
   - Add dependencies: ILogger, WalManager, CursorValidator, CursorRecoveryService
   - Add field: `_recoveryStats` Dictionary for tracking recovery events
   - Add method: `GetRecoveryStats()` for observability

2. **CompactionCursor**
   - File: Lumina/Core/Models/CompactionCursor.cs
   - Add properties: LastWalFileSize, LastParquetEntryCount, DataChecksum

3. **CompactionSettings**
   - File: Lumina/Core/Configuration/CompactionSettings.cs
   - Add properties: EnableCursorValidation, EnableCursorRecovery, StrictValidationMode

[Dependencies]

### New Package Dependencies

None required - using existing CRC-8 implementation and System.IO.Hashing for CRC-32 (available in .NET 8).

If CRC-32 is not available in target .NET version:
- Add package: `System.IO.Hashing` (for CRC-32)
- Alternative: Implement CRC-32 similar to existing Crc8 class

### Internal Dependencies

- CursorValidator depends on: WalManager, CompactionSettings
- CursorRecoveryService depends on: WalManager, ParquetReader, CompactionSettings, ILogger
- CursorManager depends on: CursorValidator, CursorRecoveryService, ILogger

[Testing]

### Unit Tests Required

1. **CursorFileHeaderTests**
   - Header round-trip serialization
   - Checksum computation correctness
   - Validation of corrupted headers
   - Magic number mismatch detection
   - Version mismatch handling

2. **CursorValidatorTests**
   - Valid cursor passes validation
   - Missing cursor file returns NotFound
   - Corrupted header detection
   - Checksum mismatch detection
   - Invalid JSON detection
   - Missing WAL file detection
   - Missing Parquet file detection

3. **CursorRecoveryTests**
   - Recovery from checksum corruption
   - Recovery from truncated file
   - Rebuild from WAL files
   - Rebuild from Parquet files
   - Recovery when no WAL files exist
   - Recovery when no Parquet files exist

### Integration Tests Required

1. **CursorResiliencyIntegrationTests**
   - End-to-end: corrupt cursor, restart, verify recovery
   - Corrupt cursor while system running, verify next compaction handles it
   - Delete referenced WAL file, verify cursor adjusts

### Modified Tests

1. **CursorManagerTests**
   - Update `ShouldIgnoreCorruptedCursorFiles` to expect recovery attempt
   - Add test for new file format backward compatibility
   - Add test for atomic write with header

[Implementation Order]

1. **Step 1: Create CursorFileHeader struct**
   - Create Lumina/Storage/Compaction/CursorFileHeader.cs
   - Implement header structure with magic, version, checksum, length
   - Implement CRC-32 checksum computation (reuse or create similar to Crc8)
   - Add unit tests for CursorFileHeader

2. **Step 2: Enhance CompactionCursor model**
   - Add new validation fields to CompactionCursor.cs
   - Update L1Compactor to populate new fields
   - Ensure backward compatibility with existing tests

3. **Step 3: Create CursorValidator**
   - Create Lumina/Storage/Compaction/CursorValidator.cs
   - Implement ValidateAsync for file integrity
   - Implement CrossValidateFilesAsync for filesystem validation
   - Add unit tests for CursorValidator

4. **Step 4: Create CursorRecoveryService**
   - Create Lumina/Storage/Compaction/CursorRecoveryService.cs
   - Implement RebuildFromWalFilesAsync
   - Implement RebuildFromParquetFilesAsync
   - Implement TryRecoverAsync orchestration
   - Add unit tests for CursorRecoveryService

5. **Step 5: Update CursorManager**
   - Modify LoadCursors to use new format with validation
   - Modify SaveCursor to write header + payload
   - Add recovery logic integration
   - Add logging for corruption events
   - Update existing tests

6. **Step 6: Add configuration options**
   - Update CompactionSettings with new options
   - Wire up dependency injection in Program.cs
   - Add integration tests

7. **Step 7: Documentation and final testing**
   - Add XML documentation comments
   - Run full test suite
   - Manual testing of corruption scenarios

## Suggestions for Implementation:

- Endianness Constraints: Since ExpectedMagic is defined as little-endian, ensure that CursorFileHeader.WriteTo and ReadFrom explicitly serialize using BinaryPrimitives.WriteUInt32LittleEndian / ReadUInt32LittleEndian to avoid platform-specific bugs.
- Atomic Saves: For SaveCursor, ensure the temp-file-and-rename approach flushes the FileStream (fs.Flush(true)) to disk before moving it, preventing zero-byte files on power loss.
