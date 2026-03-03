using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Parquet;
using Lumina.Storage.Wal;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Handles cursor recovery from various corruption scenarios.
/// Can rebuild cursor state by scanning WAL files or Parquet file metadata.
/// </summary>
public sealed class CursorRecoveryService
{
  private readonly WalManager _walManager;
  private readonly CompactionSettings _settings;
  private readonly ILogger<CursorRecoveryService> _logger;

  /// <summary>
  /// Initializes a new instance of the CursorRecoveryService class.
  /// </summary>
  /// <param name="walManager">The WAL manager.</param>
  /// <param name="settings">The compaction settings.</param>
  /// <param name="logger">The logger.</param>
  public CursorRecoveryService(
      WalManager walManager,
      CompactionSettings settings,
      ILogger<CursorRecoveryService> logger)
  {
    _walManager = walManager;
    _settings = settings;
    _logger = logger;
  }

  /// <summary>
  /// Attempts to recover a cursor based on the validation error.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="error">The validation error.</param>
  /// <param name="originalCursor">The original cursor if available.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A tuple containing success, the recovered cursor, and recovery info.</returns>
  public async Task<(bool Success, CompactionCursor? Cursor, CursorRecoveryInfo Info)> TryRecoverAsync(
      string stream,
      CursorValidationResult error,
      CompactionCursor? originalCursor,
      CancellationToken cancellationToken = default)
  {
    var originalOffset = originalCursor?.LastCompactedOffset ?? 0;
    CompactionCursor? recoveredCursor = null;
    string? recoveryMethod = null;

    // For file not found, create a new cursor
    if (error == CursorValidationResult.NotFound) {
      recoveredCursor = new CompactionCursor { Stream = stream };
      recoveryMethod = "NewCursor";
    }
    // For missing referenced files, adjust the cursor
    else if (error == CursorValidationResult.WalFileNotFound ||
             error == CursorValidationResult.ParquetFileNotFound) {
      recoveredCursor = await RebuildFromExistingFilesAsync(stream, cancellationToken);
      recoveryMethod = recoveredCursor != null ? "FileAdjustment" : null;
    }
    // For corruption, try to rebuild from WAL/Parquet
    else if (CursorValidator.IsCorruption(error)) {
      // First try to rebuild from Parquet files (most reliable)
      recoveredCursor = await RebuildFromParquetFilesAsync(stream, cancellationToken);
      recoveryMethod = recoveredCursor != null ? "ParquetScan" : null;

      // If no Parquet files, try WAL files
      if (recoveredCursor == null) {
        recoveredCursor = await RebuildFromWalFilesAsync(stream, cancellationToken);
        recoveryMethod = recoveredCursor != null ? "WalScan" : null;
      }

      // Last resort: create a new cursor
      if (recoveredCursor == null) {
        recoveredCursor = new CompactionCursor { Stream = stream };
        recoveryMethod = "NewCursor";
      }
    }
    // For unsupported version, try to migrate or rebuild
    else if (error == CursorValidationResult.UnsupportedVersion) {
      recoveredCursor = await RebuildFromParquetFilesAsync(stream, cancellationToken);
      recoveryMethod = recoveredCursor != null ? "MigrationFromParquet" : null;

      if (recoveredCursor == null) {
        recoveredCursor = await RebuildFromWalFilesAsync(stream, cancellationToken);
        recoveryMethod = recoveredCursor != null ? "MigrationFromWal" : null;
      }

      if (recoveredCursor == null) {
        recoveredCursor = new CompactionCursor { Stream = stream };
        recoveryMethod = "NewCursor";
      }
    }

    var info = new CursorRecoveryInfo {
      Stream = stream,
      ValidationResult = error,
      WasRecovered = recoveredCursor != null,
      RecoveryMethod = recoveryMethod,
      OriginalOffset = originalOffset,
      RecoveredOffset = recoveredCursor?.LastCompactedOffset ?? 0,
      RecoveryTimestamp = DateTime.UtcNow
    };

    if (recoveredCursor != null && originalOffset != recoveredCursor.LastCompactedOffset) {
      _logger.LogWarning(
          "Cursor recovery for stream {Stream}: {Method}, offset changed from {OriginalOffset} to {RecoveredOffset}",
          stream, recoveryMethod, originalOffset, recoveredCursor.LastCompactedOffset);
    }

    return (recoveredCursor != null, recoveredCursor, info);
  }

  /// <summary>
  /// Rebuilds cursor state by scanning WAL files to find the latest compactable position.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The rebuilt cursor, or null if no WAL files exist.</returns>
  public async Task<CompactionCursor?> RebuildFromWalFilesAsync(
      string stream,
      CancellationToken cancellationToken = default)
  {
    var walFiles = _walManager.GetWalFiles(stream);
    if (walFiles.Count == 0) {
      _logger.LogDebug("No WAL files found for stream {Stream} during recovery", stream);
      return null;
    }

    // Get the active WAL file (should not be compacted yet)
    var activeWalPath = _walManager.GetActiveWriterFilePath(stream);

    // Find the latest sealed WAL file (the one we should consider as the last compacted)
    string? lastWalFile = null;
    long lastOffset = 0;
    long lastFileSize = 0;

    foreach (var walFile in walFiles) {
      // Skip the active WAL file
      if (string.Equals(walFile, activeWalPath, StringComparison.OrdinalIgnoreCase)) {
        continue;
      }

      try {
        var fileInfo = new FileInfo(walFile);
        using var reader = await _walManager.GetReaderAsync(walFile, stream, cancellationToken);

        // Read to the end to find the last valid entry
        long lastEntryOffset = 0;
        await foreach (var entry in reader.ReadEntriesAsync(cancellationToken)) {
          lastEntryOffset = entry.Offset;
        }

        // Only update if this file is newer (by filename comparison)
        if (lastWalFile == null || string.Compare(walFile, lastWalFile, StringComparison.Ordinal) > 0) {
          lastWalFile = walFile;
          lastOffset = lastEntryOffset;
          lastFileSize = fileInfo.Length;
        }
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to read WAL file {File} during recovery", walFile);
      }
    }

    if (lastWalFile == null) {
      // Only active WAL exists, nothing compacted yet
      _logger.LogDebug("Only active WAL file exists for stream {Stream}, creating fresh cursor", stream);
      return new CompactionCursor { Stream = stream };
    }

    _logger.LogInformation(
        "Rebuilt cursor from WAL files for stream {Stream}: file={File}, offset={Offset}",
        stream, Path.GetFileName(lastWalFile), lastOffset);

    return new CompactionCursor {
      Stream = stream,
      LastCompactedWalFile = lastWalFile,
      LastCompactedOffset = lastOffset,
      LastCompactionTime = DateTime.UtcNow,
      LastWalFileSize = lastFileSize
    };
  }

  /// <summary>
  /// Rebuilds cursor state by scanning Parquet file metadata.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The rebuilt cursor, or null if no Parquet files exist.</returns>
  public async Task<CompactionCursor?> RebuildFromParquetFilesAsync(
      string stream,
      CancellationToken cancellationToken = default)
  {
    var l1Dir = Path.Combine(_settings.L1Directory, stream);
    if (!Directory.Exists(l1Dir)) {
      _logger.LogDebug("No L1 directory found for stream {Stream} during recovery", stream);
      return null;
    }

    var parquetFiles = Directory.GetFiles(l1Dir, "*.parquet")
        .OrderBy(f => f)
        .ToList();

    if (parquetFiles.Count == 0) {
      _logger.LogDebug("No Parquet files found for stream {Stream} during recovery", stream);
      return null;
    }

    // Find the most recent Parquet file
    string? lastParquetFile = null;
    int entryCount = 0;
    DateTime lastCompactionTime = DateTime.MinValue;

    foreach (var parquetFile in parquetFiles) {
      try {
        var fileInfo = new FileInfo(parquetFile);
        var entries = await ParquetReader.ReadBatchAsync(parquetFile, cancellationToken);

        // Only update if this file is newer
        if (lastParquetFile == null || string.Compare(parquetFile, lastParquetFile, StringComparison.Ordinal) > 0) {
          lastParquetFile = parquetFile;
          entryCount = entries.Count;
          lastCompactionTime = fileInfo.CreationTimeUtc;
        }
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to read Parquet file {File} during recovery", parquetFile);
      }
    }

    if (lastParquetFile == null) {
      return null;
    }

    // Try to correlate with WAL files to get the last compacted position
    var walFiles = _walManager.GetWalFiles(stream);
    string? lastWalFile = null;
    long lastOffset = 0;

    // Find WAL files that are older than or equal to the Parquet file creation
    // These represent the WAL data that was compacted into Parquet
    foreach (var walFile in walFiles) {
      try {
        var fileInfo = new FileInfo(walFile);
        if (fileInfo.CreationTimeUtc <= lastCompactionTime) {
          if (lastWalFile == null || string.Compare(walFile, lastWalFile, StringComparison.Ordinal) > 0) {
            using var reader = await _walManager.GetReaderAsync(walFile, stream, cancellationToken);
            long lastEntryOffset = 0;
            await foreach (var entry in reader.ReadEntriesAsync(cancellationToken)) {
              lastEntryOffset = entry.Offset;
            }

            lastWalFile = walFile;
            lastOffset = lastEntryOffset;
          }
        }
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to read WAL file {File} during Parquet-based recovery", walFile);
      }
    }

    _logger.LogInformation(
        "Rebuilt cursor from Parquet files for stream {Stream}: parquet={Parquet}, wal={Wal}, offset={Offset}",
        stream, Path.GetFileName(lastParquetFile), lastWalFile != null ? Path.GetFileName(lastWalFile) : "none", lastOffset);

    return new CompactionCursor {
      Stream = stream,
      LastCompactedWalFile = lastWalFile,
      LastCompactedOffset = lastOffset,
      LastCompactionTime = lastCompactionTime,
      LastParquetFile = lastParquetFile,
      LastParquetEntryCount = entryCount
    };
  }

  /// <summary>
  /// Rebuilds cursor by finding the most recent valid state from existing files.
  /// Used when referenced files are missing but cursor was otherwise valid.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The adjusted cursor.</returns>
  public async Task<CompactionCursor?> RebuildFromExistingFilesAsync(
      string stream,
      CancellationToken cancellationToken = default)
  {
    // First try Parquet files (most reliable indicator of compaction progress)
    var parquetCursor = await RebuildFromParquetFilesAsync(stream, cancellationToken);
    if (parquetCursor != null && !string.IsNullOrEmpty(parquetCursor.LastParquetFile)) {
      return parquetCursor;
    }

    // Fall back to WAL files
    var walCursor = await RebuildFromWalFilesAsync(stream, cancellationToken);
    if (walCursor != null) {
      return walCursor;
    }

    // Nothing found, create fresh cursor
    return new CompactionCursor { Stream = stream };
  }

  /// <summary>
  /// Performs a full integrity check and recovery for all streams.
  /// </summary>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A dictionary of recovery info by stream.</returns>
  public async Task<Dictionary<string, CursorRecoveryInfo>> ValidateAndRecoverAllAsync(
      CancellationToken cancellationToken = default)
  {
    var results = new Dictionary<string, CursorRecoveryInfo>();
    var streams = _walManager.GetActiveStreams();

    foreach (var stream in streams) {
      var cursorFilePath = Path.Combine(_settings.CursorDirectory, $"{stream}.cursor");
      var validator = new CursorValidator();
      var (result, cursor) = await validator.ValidateAsync(cursorFilePath, cancellationToken);

      if (result != CursorValidationResult.Valid) {
        var (success, recoveredCursor, info) = await TryRecoverAsync(stream, result, cursor, cancellationToken);
        results[stream] = info;
      }
    }

    return results;
  }
}