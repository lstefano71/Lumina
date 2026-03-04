using Lumina.Core.Models;

using System.Collections.Concurrent;

namespace Lumina.Storage.Wal;

/// <summary>
/// Represents a single buffered entry awaiting compaction.
/// </summary>
public sealed class BufferedEntry
{
  /// <summary>
  /// Gets the WAL file path that contains this entry.
  /// </summary>
  public required string WalFile { get; init; }

  /// <summary>
  /// Gets the byte offset in the WAL file where this entry starts.
  /// </summary>
  public required long Offset { get; init; }

  /// <summary>
  /// Gets the deserialized log entry.
  /// </summary>
  public required LogEntry LogEntry { get; init; }
}

/// <summary>
/// In-memory hot buffer for WAL entries that have not yet been compacted to Parquet.
/// Provides sub-second query visibility for recently ingested data.
/// Thread-safe for concurrent writes and reads.
/// </summary>
public sealed class WalHotBuffer
{
  private readonly ConcurrentDictionary<string, StreamBuffer> _buffers = new(StringComparer.OrdinalIgnoreCase);
  private long _version;

  /// <summary>
  /// Gets the current version counter. Incremented on every mutation (append, evict).
  /// Consumers can compare this to a cached value to detect changes.
  /// </summary>
  public long Version => Interlocked.Read(ref _version);

  /// <summary>
  /// Gets the total number of buffered entries across all streams.
  /// </summary>
  public int TotalEntries => _buffers.Values.Sum(b => b.Count);

  /// <summary>
  /// Gets the per-stream version for change detection.
  /// Returns 0 if the stream has no buffer.
  /// </summary>
  public long GetStreamVersion(string stream)
  {
    return _buffers.TryGetValue(stream, out var buffer) ? buffer.Version : 0;
  }

  /// <summary>
  /// Appends a single entry to the buffer for the given stream.
  /// Called on the hot write path after each WAL write.
  /// </summary>
  public void Append(string stream, string walFile, long offset, LogEntry entry)
  {
    var buffer = _buffers.GetOrAdd(stream, _ => new StreamBuffer());
    buffer.Append(new BufferedEntry {
      WalFile = walFile,
      Offset = offset,
      LogEntry = entry
    });
    Interlocked.Increment(ref _version);
  }

  /// <summary>
  /// Appends a batch of entries. Used during startup WAL replay.
  /// </summary>
  public void AppendBatch(string stream, IReadOnlyList<BufferedEntry> entries)
  {
    if (entries.Count == 0) return;

    var buffer = _buffers.GetOrAdd(stream, _ => new StreamBuffer());
    buffer.AppendBatch(entries);
    Interlocked.Increment(ref _version);
  }

  /// <summary>
  /// Returns a point-in-time snapshot of all buffered entries for a stream.
  /// The returned list is safe to iterate while new entries are being appended.
  /// </summary>
  public IReadOnlyList<BufferedEntry> TakeSnapshot(string stream)
  {
    if (!_buffers.TryGetValue(stream, out var buffer)) {
      return Array.Empty<BufferedEntry>();
    }

    return buffer.TakeSnapshot();
  }

  /// <summary>
  /// Evicts all entries that are at or before the compaction cursor.
  /// Called by the L1 compactor after MarkCompactionComplete.
  /// </summary>
  public void EvictCompacted(string stream, string lastCompactedWalFile, long lastCompactedOffset)
  {
    if (!_buffers.TryGetValue(stream, out var buffer)) return;

    buffer.EvictCompacted(lastCompactedWalFile, lastCompactedOffset);
    Interlocked.Increment(ref _version);
  }

  /// <summary>
  /// Gets the list of streams that have buffered entries.
  /// </summary>
  public IReadOnlyList<string> GetBufferedStreams()
  {
    return _buffers
        .Where(kv => kv.Value.Count > 0)
        .Select(kv => kv.Key)
        .ToList();
  }

  /// <summary>
  /// Per-stream buffer with its own locking for minimal contention.
  /// </summary>
  private sealed class StreamBuffer
  {
    private readonly Lock _lock = new();
    private List<BufferedEntry> _entries = new();
    private long _version;

    public int Count {
      get {
        lock (_lock) {
          return _entries.Count;
        }
      }
    }

    public long Version => Interlocked.Read(ref _version);

    public void Append(BufferedEntry entry)
    {
      lock (_lock) {
        _entries.Add(entry);
        Interlocked.Increment(ref _version);
      }
    }

    public void AppendBatch(IReadOnlyList<BufferedEntry> entries)
    {
      lock (_lock) {
        _entries.AddRange(entries);
        Interlocked.Increment(ref _version);
      }
    }

    public IReadOnlyList<BufferedEntry> TakeSnapshot()
    {
      lock (_lock) {
        return _entries.ToList();
      }
    }

    public void EvictCompacted(string lastCompactedWalFile, long lastCompactedOffset)
    {
      lock (_lock) {
        _entries = _entries.Where(e => !IsCompacted(e, lastCompactedWalFile, lastCompactedOffset)).ToList();
        Interlocked.Increment(ref _version);
      }
    }

    private static bool IsCompacted(BufferedEntry entry, string lastCompactedWalFile, long lastCompactedOffset)
    {
      int cmp = string.Compare(entry.WalFile, lastCompactedWalFile, StringComparison.Ordinal);
      if (cmp < 0) return true;  // entry is in an older (fully compacted) file
      if (cmp > 0) return false; // entry is in a newer file
      return entry.Offset <= lastCompactedOffset; // same file: compare offset
    }
  }
}
