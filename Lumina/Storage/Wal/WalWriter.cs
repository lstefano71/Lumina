using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Serialization;

using Microsoft.Win32.SafeHandles;

using System.Buffers;

namespace Lumina.Storage.Wal;

/// <summary>
/// Writes log entries to a Write-Ahead Log file with durability guarantees.
/// </summary>
public sealed class WalWriter : IAsyncDisposable
{
  private readonly FileStream _fileStream;
  private readonly SafeFileHandle _handle;
  private readonly WalSettings _settings;
  private readonly string _filePath;
  private readonly string _stream;
  private long _currentOffset;
  private bool _headerWritten;
  private bool _disposed;

  /// <summary>
  /// Gets the current file path.
  /// </summary>
  public string FilePath => _filePath;

  /// <summary>
  /// Gets the current file size in bytes (tracks the logical write position).
  /// </summary>
  public long FileSize => Interlocked.Read(ref _currentOffset);

  /// <summary>
  /// Gets the stream name this writer belongs to.
  /// </summary>
  public string Stream => _stream;

  /// <summary>
  /// Initializes a new instance of the WalWriter class.
  /// </summary>
  /// <param name="filePath">The path to the WAL file.</param>
  /// <param name="stream">The stream name.</param>
  /// <param name="settings">The WAL settings.</param>
  /// <param name="createNew">Whether to create a new file or append to existing.</param>
  private WalWriter(FileStream fileStream, string filePath, string stream, WalSettings settings, bool createNew)
  {
    _fileStream = fileStream;
    _handle = fileStream.SafeFileHandle;
    _filePath = filePath;
    _stream = stream;
    _settings = settings;
    _currentOffset = createNew ? 0 : fileStream.Length;
    _headerWritten = !createNew && _currentOffset >= WalFileHeader.Size;
  }

  /// <summary>
  /// Creates a new WalWriter for the specified file.
  /// </summary>
  /// <param name="filePath">The path to the WAL file.</param>
  /// <param name="stream">The stream name.</param>
  /// <param name="settings">The WAL settings.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A new WalWriter instance.</returns>
  public static async Task<WalWriter> CreateAsync(
      string filePath,
      string stream,
      WalSettings settings,
      CancellationToken cancellationToken = default)
  {
    var directory = Path.GetDirectoryName(filePath);
    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory)) {
      Directory.CreateDirectory(directory);
    }

    var createNew = !File.Exists(filePath) || new FileInfo(filePath).Length == 0;

    var fileStream = new FileStream(
        filePath,
        FileMode.OpenOrCreate,
        FileAccess.ReadWrite,
        FileShare.Read,
        bufferSize: 65536,
        options: settings.EnableWriteThrough ? FileOptions.WriteThrough : FileOptions.None);

    var writer = new WalWriter(fileStream, filePath, stream, settings, createNew);

    if (createNew) {
      await writer.WriteFileHeaderAsync(cancellationToken);
    }

    return writer;
  }

  /// <summary>
  /// Writes a single log entry to the WAL.
  /// </summary>
  /// <param name="entry">The log entry to write.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The offset at which the entry was written.</returns>
  public async ValueTask<long> WriteAsync(LogEntry entry, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    EnsureHeaderWritten();

    var payload = LogEntrySerializer.Serialize(entry);
    var frameHeader = new WalFrameHeader((uint)payload.Length, WalEntryType.StandardLog);

    var totalSize = WalFrameHeader.Size + payload.Length;
    var buffer = ArrayPool<byte>.Shared.Rent(totalSize);

    try {
      // Write frame header
      frameHeader.WriteTo(buffer.AsSpan(0, WalFrameHeader.Size));

      // Write payload
      payload.CopyTo(buffer, WalFrameHeader.Size);

      // Atomically reserve space in the file so concurrent writers
      // each get their own non-overlapping region.
      var offset = Interlocked.Add(ref _currentOffset, totalSize) - totalSize;

      try {
        // Write at the reserved offset (safe for concurrent callers).
        await RandomAccess.WriteAsync(_handle, buffer.AsMemory(0, totalSize), offset, cancellationToken);
        await _fileStream.FlushAsync(cancellationToken);
      } catch {
        // The offset space has already been reserved. Write a padding frame so
        // readers can cleanly skip this region instead of hitting garbage bytes.
        WritePaddingFrame(buffer, totalSize, offset);
        throw;
      }

      return offset;
    } finally {
      ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  /// <summary>
  /// Writes a batch of log entries to the WAL with a single fsync.
  /// </summary>
  /// <param name="entries">The log entries to write.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The offsets at which entries were written.</returns>
  public async ValueTask<long[]> WriteBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);

    if (entries.Count == 0) {
      return Array.Empty<long>();
    }

    EnsureHeaderWritten();

    // Pre-calculate total size needed
    var payloads = new byte[entries.Count][];
    var totalSize = 0;

    for (int i = 0; i < entries.Count; i++) {
      payloads[i] = LogEntrySerializer.Serialize(entries[i]);
      totalSize += WalFrameHeader.Size + payloads[i].Length;
    }

    var buffer = ArrayPool<byte>.Shared.Rent(totalSize);
    var offsets = new long[entries.Count];

    try {
      // Atomically reserve the entire block for all entries at once.
      var baseOffset = Interlocked.Add(ref _currentOffset, totalSize) - totalSize;

      var bufferOffset = 0;

      for (int i = 0; i < entries.Count; i++) {
        offsets[i] = baseOffset + bufferOffset;

        var frameHeader = new WalFrameHeader((uint)payloads[i].Length, WalEntryType.StandardLog);
        frameHeader.WriteTo(buffer.AsSpan(bufferOffset, WalFrameHeader.Size));
        bufferOffset += WalFrameHeader.Size;

        payloads[i].CopyTo(buffer, bufferOffset);
        bufferOffset += payloads[i].Length;
      }

      try {
        // Write at the reserved offset (safe for concurrent callers).
        await RandomAccess.WriteAsync(_handle, buffer.AsMemory(0, totalSize), baseOffset, cancellationToken);
        await _fileStream.FlushAsync(cancellationToken);
      } catch {
        // The offset space has already been reserved. Write a padding frame so
        // readers can cleanly skip this region instead of hitting garbage bytes.
        WritePaddingFrame(buffer, totalSize, baseOffset);
        throw;
      }

      return offsets;
    } finally {
      ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  /// <summary>
  /// Forces a flush of the underlying buffer to disk.
  /// </summary>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
  {
    ObjectDisposedException.ThrowIf(_disposed, this);
    await _fileStream.FlushAsync(cancellationToken);
  }

  /// <summary>
  /// Checks if the WAL file needs rotation based on size.
  /// </summary>
  /// <returns>True if rotation is needed.</returns>
  public bool NeedsRotation()
  {
    return _currentOffset >= _settings.MaxWalSizeBytes;
  }

  private async Task WriteFileHeaderAsync(CancellationToken cancellationToken)
  {
    var header = WalFileHeader.CreateDefault();
    var buffer = new byte[WalFileHeader.Size];
    header.WriteTo(buffer);

    // Header is written once during CreateAsync before the writer is
    // shared, so no contention here.
    await RandomAccess.WriteAsync(_handle, buffer, 0, cancellationToken);
    await _fileStream.FlushAsync(cancellationToken);
    _currentOffset = WalFileHeader.Size;
    _headerWritten = true;
  }

  private void EnsureHeaderWritten()
  {
    if (!_headerWritten) {
      throw new InvalidOperationException("WAL file header has not been written.");
    }
  }

  /// <summary>
  /// Writes a padding frame at the given offset so that readers can cleanly skip
  /// a region whose original write failed after offset space was reserved.
  /// This is best-effort; if it also fails the region remains as garbage bytes
  /// which the reader's ScanToNextValidFrame logic will skip.
  /// </summary>
  private void WritePaddingFrame(byte[] buffer, int totalSize, long offset)
  {
    try {
      var paddingPayloadSize = totalSize - WalFrameHeader.Size;
      if (paddingPayloadSize < 0) return;

      var padHeader = new WalFrameHeader((uint)paddingPayloadSize, WalEntryType.Padding);
      padHeader.WriteTo(buffer.AsSpan(0, WalFrameHeader.Size));

      // Zero out the payload region so the file doesn't contain leftover data
      buffer.AsSpan(WalFrameHeader.Size, paddingPayloadSize).Clear();

      RandomAccess.Write(_handle, buffer.AsSpan(0, totalSize), offset);
    } catch {
      // Best effort — the reader's scan logic will handle remaining garbage.
    }
  }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    if (_disposed) {
      return;
    }

    _disposed = true;

    try {
      await _fileStream.FlushAsync();
    } catch (ObjectDisposedException) {
      // Already disposed, ignore
    }

    await _fileStream.DisposeAsync();
  }
}