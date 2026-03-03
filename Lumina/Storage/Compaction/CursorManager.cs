using Lumina.Core.Models;

using System.Text.Json;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Manages compaction cursors for tracking WAL → Parquet progress.
/// Ensures idempotent compaction with exactly-once semantics.
/// </summary>
public sealed class CursorManager
{
  private readonly string _cursorDirectory;
  private readonly object _lock = new();
  private readonly Dictionary<string, CompactionCursor> _cursors = new();

  public CursorManager(string cursorDirectory)
  {
    _cursorDirectory = cursorDirectory;

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
          LastParquetFile = cursor.LastParquetFile
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
  /// Marks a compaction batch as complete.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="lastWalFile">The last WAL file processed.</param>
  /// <param name="lastOffset">The last offset compacted.</param>
  /// <param name="parquetFile">The output Parquet file path.</param>
  public void MarkCompactionComplete(string stream, string lastWalFile, long lastOffset, string parquetFile)
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

  private void LoadCursors()
  {
    foreach (var file in Directory.GetFiles(_cursorDirectory, "*.cursor.json")) {
      try {
        var json = File.ReadAllText(file);
        var cursor = JsonSerializer.Deserialize<CompactionCursor>(json);

        if (cursor != null) {
          _cursors[cursor.Stream] = cursor;
        }
      } catch {
        // Ignore corrupted cursor files
      }
    }
  }

  private void SaveCursor(CompactionCursor cursor)
  {
    var filePath = Path.Combine(_cursorDirectory, $"{cursor.Stream}.cursor.json");
    var json = JsonSerializer.Serialize(cursor, new JsonSerializerOptions { WriteIndented = true });

    // Write to temp file first, then move for atomicity
    var tempPath = filePath + ".tmp";
    File.WriteAllText(tempPath, json);
    File.Move(tempPath, filePath, true);
  }
}