using Lumina.Core.Configuration;
using Lumina.Core.Models;

using System.Collections.Concurrent;

namespace Lumina.Storage.Wal;

/// <summary>
/// Manages WAL writers and file rotation for multiple streams.
/// </summary>
public sealed class WalManager : IAsyncDisposable
{
  private readonly WalSettings _settings;
  private readonly ConcurrentDictionary<string, WalWriter> _writers = new();
  private readonly ConcurrentBag<WalWriter> _rotatedWriters = new();
  private readonly string _baseDirectory;
  private bool _disposed;

  /// <summary>
  /// Gets the base data directory.
  /// </summary>
  public string BaseDirectory => _baseDirectory;

  /// <summary>
  /// Initializes a new instance of the WalManager class.
  /// </summary>
  /// <param name="settings">The WAL settings.</param>
  public WalManager(WalSettings settings)
  {
    _settings = settings;
    _baseDirectory = Path.GetFullPath(settings.DataDirectory);

    if (!Directory.Exists(_baseDirectory)) {
      Directory.CreateDirectory(_baseDirectory);
    }
  }

  /// <summary>
  /// Gets or creates a WAL writer for the specified stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The WAL writer for the stream.</returns>
  public async Task<WalWriter> GetOrCreateWriterAsync(string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    // Validate stream name
    ValidateStreamName(stream);

    if (_writers.TryGetValue(stream, out var existingWriter)) {
      return existingWriter;
    }

    // Create new writer
    var filePath = GetWalPath(stream, DateTime.UtcNow);
    var newWriter = await WalWriter.CreateAsync(filePath, stream, _settings, cancellationToken);

    var actual = _writers.GetOrAdd(stream, newWriter);
    if (actual != newWriter) {
      // Another thread won the race; dispose our writer to avoid leaking file handles
      await newWriter.DisposeAsync();
    }
    return actual;
  }

  /// <summary>
  /// Checks if WAL rotation is needed for the specified stream and performs it.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The path of the rotated file, or null if no rotation occurred.</returns>
  public async Task<string?> RotateWalIfNeededAsync(string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (!_writers.TryGetValue(stream, out var writer)) {
      return null;
    }

    if (!writer.NeedsRotation()) {
      return null;
    }

    // Get the current file path before rotation
    var oldPath = writer.FilePath;

    // Create a new writer BEFORE swapping to avoid disposing one still in use
    var newFilePath = GetWalPath(stream, DateTime.UtcNow);
    var newWriter = await WalWriter.CreateAsync(newFilePath, stream, _settings, cancellationToken);

    // Atomic swap: only succeeds if the writer hasn't been rotated by another thread
    if (_writers.TryUpdate(stream, newWriter, writer)) {
      // Flush the old writer so all data is persisted, then defer its disposal
      try { await writer.FlushAsync(); } catch (ObjectDisposedException) { }
      _rotatedWriters.Add(writer);
    } else {
      // Another thread already rotated; dispose the writer we just created
      await newWriter.DisposeAsync();
    }

    return oldPath;
  }

  /// <summary>
  /// Forces rotation of the WAL for the specified stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The path of the rotated file.</returns>
  public async Task<string?> ForceRotateAsync(string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (!_writers.TryGetValue(stream, out var writer)) {
      return null;
    }

    var oldPath = writer.FilePath;

    // Create a new writer BEFORE swapping
    var newFilePath = GetWalPath(stream, DateTime.UtcNow);
    var newWriter = await WalWriter.CreateAsync(newFilePath, stream, _settings, cancellationToken);

    if (_writers.TryUpdate(stream, newWriter, writer)) {
      // Force rotate is an explicit operation; dispose the old writer immediately
      // so its file handle is released and the file can be deleted by compaction.
      await writer.DisposeAsync();
    } else {
      await newWriter.DisposeAsync();
    }

    return oldPath;
  }

  /// <summary>
  /// Gets all WAL files for the specified stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>A list of WAL file paths.</returns>
  public IReadOnlyList<string> GetWalFiles(string stream)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    var streamDir = GetStreamDirectory(stream);
    if (!Directory.Exists(streamDir)) {
      return Array.Empty<string>();
    }

    return Directory.GetFiles(streamDir, "*.wal")
        .OrderBy(f => f)
        .ToList();
  }

  /// <summary>
  /// Gets all WAL files across all streams.
  /// </summary>
  /// <returns>A dictionary mapping stream names to their WAL files.</returns>
  public IReadOnlyDictionary<string, IReadOnlyList<string>> GetAllWalFiles()
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    var result = new Dictionary<string, IReadOnlyList<string>>();

    if (!Directory.Exists(_baseDirectory)) {
      return result;
    }

    foreach (var streamDir in Directory.GetDirectories(_baseDirectory)) {
      var streamName = Path.GetFileName(streamDir);
      var walFiles = Directory.GetFiles(streamDir, "*.wal")
          .OrderBy(f => f)
          .ToList();

      if (walFiles.Count > 0) {
        result[streamName] = walFiles;
      }
    }

    return result;
  }

  /// <summary>
  /// Gets a reader for the specified WAL file.
  /// </summary>
  /// <param name="filePath">The WAL file path.</param>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A WalReader instance.</returns>
  public Task<WalReader> GetReaderAsync(string filePath, string stream, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    return WalReader.CreateAsync(filePath, stream, cancellationToken);
  }

  /// <summary>
  /// Gets the file path of the active WAL writer for a stream, or null if no writer is open.
  /// This file must not be deleted during compaction as it is still being written to.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>The active WAL file path, or null.</returns>
  public string? GetActiveWriterFilePath(string stream)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    return _writers.TryGetValue(stream, out var writer) ? writer.FilePath : null;
  }

  /// <summary>
  /// Gets the current WAL file size for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>The current file size in bytes, or 0 if no writer exists.</returns>
  public long GetCurrentWalSize(string stream)
  {
    if (_writers.TryGetValue(stream, out var writer)) {
      return writer.FileSize;
    }
    return 0;
  }

  /// <summary>
  /// Deletes old WAL files after they have been compacted.
  /// </summary>
  /// <param name="filePath">The file to delete.</param>
  /// <returns>True if the file was deleted.</returns>
  public bool DeleteWalFile(string filePath)
  {
    try {
      if (File.Exists(filePath)) {
        File.Delete(filePath);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }

  /// <summary>
  /// Gets the list of active stream names.
  /// </summary>
  /// <returns>List of stream names that have WAL files.</returns>
  public IReadOnlyList<string> GetActiveStreams()
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (!Directory.Exists(_baseDirectory)) {
      return Array.Empty<string>();
    }

    return Directory.GetDirectories(_baseDirectory)
        .Select(Path.GetFileName)
        .Where(s => !string.IsNullOrEmpty(s))
        .Cast<string>()
        .ToList();
  }

  /// <summary>
  /// Reads all entries from all WAL files for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>An async enumerable of log entries.</returns>
  public async IAsyncEnumerable<LogEntry> ReadEntriesAsync(
      string stream,
      [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    var walFiles = GetWalFiles(stream);

    foreach (var file in walFiles) {
      if (new FileInfo(file).Length < WalFileHeader.Size) {
        continue;
      }

      using var reader = await GetReaderAsync(file, stream, cancellationToken);

      await foreach (var walEntry in reader.ReadEntriesAsync(cancellationToken)) {
        walEntry.LogEntry.Offset = walEntry.Offset;
        yield return walEntry.LogEntry;
      }
    }
  }

  /// <summary>
  /// Gets the stream directory path.
  /// </summary>
  private string GetStreamDirectory(string stream)
  {
    return Path.Combine(_baseDirectory, stream);
  }

  /// <summary>
  /// Gets the WAL file path for a stream with a timestamp.
  /// </summary>
  private string GetWalPath(string stream, DateTime timestamp)
  {
    var streamDir = GetStreamDirectory(stream);

    if (!Directory.Exists(streamDir)) {
      Directory.CreateDirectory(streamDir);
    }

    // Format: YYYYMMDD_HHmmss_{sequence}.wal
    var baseName = timestamp.ToString("yyyyMMdd_HHmmss");
    var sequence = 0;
    string filePath;

    do {
      filePath = Path.Combine(streamDir, $"{baseName}_{sequence:D4}.wal");
      sequence++;
    }
    while (File.Exists(filePath));

    return filePath;
  }

  /// <summary>
  /// Validates a stream name for filesystem compatibility.
  /// </summary>
  private static void ValidateStreamName(string stream)
  {
    if (string.IsNullOrWhiteSpace(stream)) {
      throw new ArgumentException("Stream name cannot be empty.", nameof(stream));
    }

    var invalidChars = Path.GetInvalidFileNameChars();
    if (stream.IndexOfAny(invalidChars) >= 0) {
      throw new ArgumentException($"Stream name contains invalid characters: {stream}", nameof(stream));
    }

    if (stream.Length > 100) {
      throw new ArgumentException("Stream name is too long (max 100 characters).", nameof(stream));
    }
  }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    if (_disposed) {
      return;
    }

    _disposed = true;

    // Dispose all active writers
    foreach (var writer in _writers.Values) {
      await writer.DisposeAsync();
    }

    _writers.Clear();

    // Dispose rotated writers that were kept alive for in-flight writes
    while (_rotatedWriters.TryTake(out var old)) {
      await old.DisposeAsync();
    }
  }
}

/// <summary>
/// Extension methods for ConcurrentDictionary.
/// </summary>
internal static class ConcurrentDictionaryExtensions
{
  public static async Task<TValue> GetOrAddAsync<TKey, TValue, TArg>(
      this ConcurrentDictionary<TKey, TValue> dictionary,
      TKey key,
      Func<TKey, TArg, Task<TValue>> valueFactory,
      TArg arg) where TKey : notnull
  {
    if (dictionary.TryGetValue(key, out var value)) {
      return value;
    }

    var newValue = await valueFactory(key, arg);
    return dictionary.GetOrAdd(key, newValue);
  }
}