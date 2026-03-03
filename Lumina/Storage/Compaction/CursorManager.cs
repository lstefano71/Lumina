using Lumina.Core.Models;

using System.Text.Json;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Manages compaction cursors for tracking WAL → Parquet progress.
/// Ensures idempotent compaction with exactly-once semantics.
/// Uses checksum-protected files with validation and recovery support.
/// </summary>
public sealed class CursorManager
{
  private readonly string _cursorDirectory;
  private readonly CursorValidator _validator;
  private readonly CursorRecoveryService? _recoveryService;
  private readonly ILogger<CursorManager>? _logger;
  private readonly object _lock = new();
  private readonly Dictionary<string, CompactionCursor> _cursors = new();
  private readonly Dictionary<string, CursorRecoveryInfo> _recoveryStats = new();
  private readonly bool _enableValidation;
  private readonly bool _enableRecovery;

  private static readonly JsonSerializerOptions JsonOptions = new() {
    WriteIndented = true,
    PropertyNameCaseInsensitive = true
  };

  /// <summary>
  /// Initializes a new instance of the CursorManager class.
  /// </summary>
  /// <param name="cursorDirectory">The directory to store cursor files.</param>
  /// <param name="validator">The cursor validator.</param>
  /// <param name="recoveryService">Optional recovery service for corrupted cursors.</param>
  /// <param name="logger">Optional logger.</param>
  /// <param name="enableValidation">Whether to enable validation (default: true).</param>
  /// <param name="enableRecovery">Whether to enable recovery (default: true).</param>
  public CursorManager(
      string cursorDirectory,
      CursorValidator validator,
      CursorRecoveryService? recoveryService = null,
      ILogger<CursorManager>? logger = null,
      bool enableValidation = true,
      bool enableRecovery = true)
  {
    _cursorDirectory = cursorDirectory;
    _validator = validator;
    _recoveryService = recoveryService;
    _logger = logger;
    _enableValidation = enableValidation;
    _enableRecovery = enableRecovery;

    if (!Directory.Exists(cursorDirectory)) {
      Directory.CreateDirectory(cursorDirectory);
    }

    LoadCursors();
  }

  /// <summary>
  /// Gets the cursor for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>The compaction cursor, or a new one if not found.</returns>
  public CompactionCursor GetCursor(string stream)
  {
    lock (_lock) {
      if (_cursors.TryGetValue(stream, out var cursor)) {
        return new CompactionCursor {
          Stream = cursor.Stream,
          LastCompactedWalFile = cursor.LastCompactedWalFile,
          LastCompactedOffset = cursor.LastCompactedOffset,
          LastCompactionTime = cursor.LastCompactionTime,
          LastParquetFile = cursor.LastParquetFile,
          LastWalFileSize = cursor.LastWalFileSize,
          LastParquetEntryCount = cursor.LastParquetEntryCount
        };
      }

      return new CompactionCursor { Stream = stream };
    }
  }

  /// <summary>
  /// Updates the cursor for a stream.
  /// </summary>
  /// <param name="cursor">The updated cursor.</param>
  public void UpdateCursor(CompactionCursor cursor)
  {
    lock (_lock) {
      _cursors[cursor.Stream] = cursor;
      SaveCursor(cursor);
    }
  }

  /// <summary>
  /// Gets all cursors.
  /// </summary>
  public IReadOnlyList<CompactionCursor> GetAllCursors()
  {
    lock (_lock) {
      return _cursors.Values.ToList();
    }
  }

  /// <summary>
  /// Gets recovery statistics for all streams.
  /// </summary>
  public IReadOnlyDictionary<string, CursorRecoveryInfo> GetRecoveryStats()
  {
    lock (_lock) {
      return new Dictionary<string, CursorRecoveryInfo>(_recoveryStats);
    }
  }

  /// <summary>
  /// Marks a compaction batch as complete.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="lastWalFile">The last WAL file processed.</param>
  /// <param name="lastOffset">The last offset compacted.</param>
  /// <param name="parquetFile">The output Parquet file path.</param>
  /// <param name="walFileSize">Optional WAL file size for validation.</param>
  /// <param name="parquetEntryCount">Optional Parquet entry count for validation.</param>
  public void MarkCompactionComplete(
      string stream,
      string lastWalFile,
      long lastOffset,
      string parquetFile,
      long? walFileSize = null,
      int? parquetEntryCount = null)
  {
    var cursor = GetCursor(stream);

    bool shouldUpdate = false;
    if (cursor.LastCompactedWalFile == null) {
      shouldUpdate = true;
    } else {
      int cmp = string.Compare(lastWalFile, cursor.LastCompactedWalFile, StringComparison.Ordinal);
      if (cmp > 0) {
        shouldUpdate = true;
      } else if (cmp == 0 && lastOffset > cursor.LastCompactedOffset) {
        shouldUpdate = true;
      }
    }

    if (shouldUpdate) {
      cursor.LastCompactedWalFile = lastWalFile;
      cursor.LastCompactedOffset = lastOffset;
      cursor.LastParquetFile = parquetFile;
      cursor.LastCompactionTime = DateTime.UtcNow;
      cursor.LastWalFileSize = walFileSize;
      cursor.LastParquetEntryCount = parquetEntryCount;

      UpdateCursor(cursor);
    }
  }

  /// <summary>
  /// Checks if a WAL file has been compacted up to the given offset (assuming it's in the same file).
  /// Note: Not safe to use without file name context. Maintained for backward compatibility.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="walFile">The WAL file name.</param>
  /// <param name="offset">The offset to check.</param>
  /// <returns>True if the offset has been compacted.</returns>
  public bool IsOffsetCompacted(string stream, string walFile, long offset)
  {
    var cursor = GetCursor(stream);

    if (cursor.LastCompactedWalFile == null) return false;

    int cmp = string.Compare(walFile, cursor.LastCompactedWalFile, StringComparison.Ordinal);
    if (cmp < 0) return true; // Older file is fully compacted
    if (cmp > 0) return false; // Newer file is not compacted

    return offset <= cursor.LastCompactedOffset; // Same file, check offset
  }

  /// <summary>
  /// Validates a cursor file and attempts recovery if needed.
  /// </summary>
  /// <param name="filePath">The cursor file path.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A tuple containing the cursor and validation result.</returns>
  public async Task<(CompactionCursor? Cursor, CursorValidationResult Result, CursorRecoveryInfo? RecoveryInfo)> ValidateAndLoadCursorAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    var (result, cursor) = await _validator.ValidateWithCrossCheckAsync(filePath, cancellationToken);

    if (result == CursorValidationResult.Valid) {
      return (cursor, result, null);
    }

    // Attempt recovery if enabled
    if (_enableRecovery && _recoveryService != null && CursorValidator.CanRecover(result)) {
      var stream = Path.GetFileNameWithoutExtension(filePath);
      if (stream.EndsWith(".cursor")) {
        stream = stream.Substring(0, stream.Length - 7);
      }

      var (success, recoveredCursor, info) = await _recoveryService.TryRecoverAsync(
          stream, result, cursor, cancellationToken);

      if (success && recoveredCursor != null) {
        lock (_lock) {
          _recoveryStats[stream] = info;
        }

        _logger?.LogWarning(
            "Recovered cursor for stream {Stream} from {Result}: {Method}",
            stream, result, info.RecoveryMethod);

        return (recoveredCursor, result, info);
      }
    }

    return (cursor, result, null);
  }

  private void LoadCursors()
  {
    foreach (var file in Directory.GetFiles(_cursorDirectory, "*.cursor")) {
      var stream = Path.GetFileNameWithoutExtension(file);
      if (stream.EndsWith(".cursor")) {
        stream = stream.Substring(0, stream.Length - 7);
      }

      try {
        if (_enableValidation) {
          // Use async validation in sync context
          var (result, cursor) = _validator.ValidateAsync(file).GetAwaiter().GetResult();

          if (result == CursorValidationResult.Valid && cursor != null) {
            _cursors[cursor.Stream] = cursor;
          } else if (_enableRecovery && _recoveryService != null && CursorValidator.CanRecover(result)) {
            var (success, recoveredCursor, info) = _recoveryService.TryRecoverAsync(
                stream, result, cursor).GetAwaiter().GetResult();

            if (success && recoveredCursor != null) {
              _cursors[recoveredCursor.Stream] = recoveredCursor;
              _recoveryStats[stream] = info;

              // Save the recovered cursor
              SaveCursor(recoveredCursor);

              _logger?.LogWarning(
                  "Recovered cursor for stream {Stream} during startup: {Method}",
                  stream, info.RecoveryMethod);
            }
          } else {
            _logger?.LogWarning(
                "Failed to load cursor for stream {Stream}: {Result}",
                stream, result);
          }
        } else {
          // Legacy loading without validation
          var cursor = LoadLegacyCursor(file);
          if (cursor != null) {
            _cursors[cursor.Stream] = cursor;
          }
        }
      } catch (Exception ex) {
        _logger?.LogWarning(ex, "Failed to load cursor file {File}", file);
      }
    }

    // Also check for old .cursor.json files and migrate them
    MigrateOldCursorFiles();
  }

  private CompactionCursor? LoadLegacyCursor(string filePath)
  {
    try {
      var json = File.ReadAllText(filePath);
      return JsonSerializer.Deserialize<CompactionCursor>(json, JsonOptions);
    } catch {
      return null;
    }
  }

  private void MigrateOldCursorFiles()
  {
    foreach (var file in Directory.GetFiles(_cursorDirectory, "*.cursor.json")) {
      try {
        var json = File.ReadAllText(file);
        var cursor = JsonSerializer.Deserialize<CompactionCursor>(json, JsonOptions);

        if (cursor != null) {
          // Save in new format
          SaveCursor(cursor);
          _cursors[cursor.Stream] = cursor;

          // Remove old file
          File.Delete(file);

          _logger?.LogInformation(
              "Migrated cursor file for stream {Stream} to new format",
              cursor.Stream);
        }
      } catch (Exception ex) {
        _logger?.LogWarning(ex, "Failed to migrate cursor file {File}", file);
      }
    }
  }

  private void SaveCursor(CompactionCursor cursor)
  {
    var filePath = Path.Combine(_cursorDirectory, $"{cursor.Stream}.cursor");
    var json = JsonSerializer.Serialize(cursor, JsonOptions);
    var payload = System.Text.Encoding.UTF8.GetBytes(json);

    // Create header with checksum
    var header = CursorFileHeader.CreateForPayload(payload);

    // Write header + payload
    var fileBytes = new byte[CursorFileHeader.Size + payload.Length];
    header.WriteTo(fileBytes);
    Array.Copy(payload, 0, fileBytes, CursorFileHeader.Size, payload.Length);

    // Write to temp file first, then move for atomicity
    var tempPath = filePath + ".tmp";
    using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None)) {
      fs.Write(fileBytes, 0, fileBytes.Length);
      fs.Flush(true); // Ensure data is written to disk
    }

    File.Move(tempPath, filePath, true);
  }
}