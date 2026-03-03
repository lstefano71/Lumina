using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Lumina.Core.Models;
using Lumina.Storage.Serialization;

namespace Lumina.Storage.Wal;

/// <summary>
/// Represents a WAL entry with metadata read from the file.
/// </summary>
public sealed class WalEntry
{
    /// <summary>
    /// Gets the stream name.
    /// </summary>
    public required string Stream { get; init; }
    
    /// <summary>
    /// Gets the file offset where this entry starts.
    /// </summary>
    public required long Offset { get; init; }
    
    /// <summary>
    /// Gets the frame header for this entry.
    /// </summary>
    public required WalFrameHeader FrameHeader { get; init; }
    
    /// <summary>
    /// Gets the deserialized log entry.
    /// </summary>
    public required LogEntry LogEntry { get; init; }
    
    /// <summary>
    /// Gets the raw payload bytes.
    /// </summary>
    public required byte[] Payload { get; init; }
}

/// <summary>
/// Reads and validates WAL entries with corruption recovery support.
/// Uses SIMD for efficient sync marker scanning.
/// </summary>
public sealed class WalReader : IDisposable
{
    private readonly FileStream _fileStream;
    private readonly string _filePath;
    private readonly string _stream;
    private bool _disposed;
    private long _fileSize;
    
    /// <summary>
    /// Gets the file path being read.
    /// </summary>
    public string FilePath => _filePath;
    
    /// <summary>
    /// Gets the stream name.
    /// </summary>
    public string Stream => _stream;
    
    /// <summary>
    /// Gets the total file size in bytes.
    /// </summary>
    public long FileSize => _fileSize;
    
    private WalReader(FileStream fileStream, string filePath, string stream, long fileSize)
    {
        _fileStream = fileStream;
        _filePath = filePath;
        _stream = stream;
        _fileSize = fileSize;
    }
    
    /// <summary>
    /// Creates a new WalReader for the specified file.
    /// </summary>
    /// <param name="filePath">The path to the WAL file.</param>
    /// <param name="stream">The stream name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new WalReader instance.</returns>
    public static async Task<WalReader> CreateAsync(
        string filePath, 
        string stream, 
        CancellationToken cancellationToken = default)
    {
        var fileStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: 65536,
            options: FileOptions.SequentialScan | FileOptions.Asynchronous);
        
        var fileSize = fileStream.Length;
        
        // Validate file header
        if (fileSize < WalFileHeader.Size)
        {
            throw new InvalidDataException($"WAL file too small: {fileSize} bytes");
        }
        
        var headerBuffer = new byte[WalFileHeader.Size];
        await fileStream.ReadExactlyAsync(headerBuffer, cancellationToken);
        
        var header = WalFileHeader.ReadFrom(headerBuffer);
        if (!header.IsValid)
        {
            throw new InvalidDataException($"Invalid WAL file header: magic=0x{header.Magic:X8}, version={header.Version}");
        }
        
        return new WalReader(fileStream, filePath, stream, fileSize);
    }
    
    /// <summary>
    /// Reads all valid entries from the WAL.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of valid WAL entries.</returns>
    public async IAsyncEnumerable<WalEntry> ReadEntriesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        _fileStream.Position = WalFileHeader.Size; // Skip file header
        
        var buffer = ArrayPool<byte>.Shared.Rent(65536);
        var frameHeaderBuffer = new byte[WalFrameHeader.Size];
        
        try
        {
            while (_fileStream.Position < _fileSize && !cancellationToken.IsCancellationRequested)
            {
                var offset = _fileStream.Position;
                
                // Read frame header
                var bytesRead = await _fileStream.ReadAsync(frameHeaderBuffer, cancellationToken);
                if (bytesRead < WalFrameHeader.Size)
                {
                    // Incomplete frame header, stop reading
                    break;
                }
                
                // Validate frame header
                if (!WalFrameHeader.TryValidate(frameHeaderBuffer, out var frameHeader))
                {
                    // Try to find next valid sync marker
                    var nextOffset = ScanToNextValidFrame(_fileStream, cancellationToken);
                    if (nextOffset < 0)
                    {
                        break; // No more valid frames found
                    }
                    
                    _fileStream.Position = nextOffset;
                    continue;
                }
                
                // Check if we have enough bytes for the payload
                var payloadEnd = _fileStream.Position + frameHeader.Length;
                if (payloadEnd > _fileSize)
                {
                    // Truncated payload, stop reading
                    break;
                }
                
                // Ensure buffer is large enough
                var payloadSize = (int)frameHeader.Length;
                if (buffer.Length < payloadSize)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = ArrayPool<byte>.Shared.Rent(payloadSize);
                }
                
                // Read payload
                bytesRead = await _fileStream.ReadAsync(buffer.AsMemory(0, payloadSize), cancellationToken);
                if (bytesRead < payloadSize)
                {
                    // Incomplete payload
                    break;
                }
                
                var payload = new byte[payloadSize];
                Array.Copy(buffer, payload, payloadSize);
                
                // Deserialize log entry
                if (!LogEntryDeserializer.TryDeserialize(payload, out var logEntry) || logEntry is null)
                {
                    // Corrupted payload, skip to next frame
                    continue;
                }
                
                yield return new WalEntry
                {
                    Stream = _stream,
                    Offset = offset,
                    FrameHeader = frameHeader,
                    LogEntry = logEntry,
                    Payload = payload
                };
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
    
    /// <summary>
    /// Scans for the next valid frame starting from the current position.
    /// Uses SIMD-optimized scanning when available.
    /// </summary>
    /// <param name="stream">The file stream.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The offset of the next valid frame, or -1 if not found.</returns>
    private static long ScanToNextValidFrame(FileStream stream, CancellationToken cancellationToken)
    {
        // Read data in chunks and search for sync marker
        var scanBuffer = new byte[65536];
        var syncMarker = WalFormat.SyncMarkerBytes;
        
        while (stream.Position < stream.Length && !cancellationToken.IsCancellationRequested)
        {
            var currentPos = stream.Position;
            var bytesToRead = Math.Min(scanBuffer.Length, (int)(stream.Length - currentPos));
            
            var bytesRead = stream.Read(scanBuffer, 0, bytesToRead);
            if (bytesRead == 0)
            {
                break;
            }
            
            // Search for sync marker using SIMD if available
            var markerOffset = ScanForSyncMarker(scanBuffer.AsSpan(0, bytesRead));
            
            if (markerOffset >= 0)
            {
                var absoluteOffset = currentPos + markerOffset;
                stream.Position = absoluteOffset;
                
                // Validate the complete frame header
                var headerBuffer = new byte[WalFrameHeader.Size];
                var headerBytesRead = stream.Read(headerBuffer, 0, WalFrameHeader.Size);
                
                if (headerBytesRead == WalFrameHeader.Size && 
                    WalFrameHeader.TryValidate(headerBuffer, out _))
                {
                    return absoluteOffset;
                }
                
                // Not a valid frame, continue scanning from after the marker
                stream.Position = absoluteOffset + 4;
            }
            else
            {
                // Move back to search across buffer boundary
                stream.Position = currentPos + bytesRead - 3;
            }
        }
        
        return -1;
    }
    
    /// <summary>
    /// Scans for the sync marker pattern in the buffer using SIMD.
    /// </summary>
    /// <param name="buffer">The buffer to scan.</param>
    /// <returns>The offset of the first sync marker, or -1 if not found.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ScanForSyncMarker(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < 4)
        {
            return -1;
        }
        
        // Use SIMD for fast scanning when available
        if (Sse2.IsSupported)
        {
            return ScanForSyncMarkerSimd(buffer);
        }
        
        // Fallback to scalar search
        return ScanForSyncMarkerScalar(buffer);
    }
    
    /// <summary>
    /// SIMD-optimized sync marker scanning using SSE2.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe int ScanForSyncMarkerSimd(ReadOnlySpan<byte> buffer)
    {
        // Pattern: 0xFA 0xCE 0xB0 0x0C
        var pattern = Vector128.Create(
            (byte)0xFA, (byte)0xCE, (byte)0xB0, (byte)0x0C,
            (byte)0xFA, (byte)0xCE, (byte)0xB0, (byte)0x0C,
            (byte)0xFA, (byte)0xCE, (byte)0xB0, (byte)0x0C,
            (byte)0xFA, (byte)0xCE, (byte)0xB0, (byte)0x0C);
        
        var mask = Vector128.Create((byte)0xFF);
        
        fixed (byte* ptr = buffer)
        {
            var i = 0;
            var length = buffer.Length - 3;
            
            // Process 16 bytes at a time
            while (i + 16 <= length)
            {
                var data = Sse2.LoadVector128(ptr + i);
                var eq0 = Sse2.CompareEqual(data, pattern);
                var masked = Sse2.And(eq0, mask);
                
                if (Sse2.MoveMask(masked) != 0)
                {
                    // Check each position for the full pattern
                    for (int j = 0; j < 16 && i + j <= length; j++)
                    {
                        if (buffer[i + j] == 0xFA && 
                            i + j + 3 < buffer.Length &&
                            buffer[i + j + 1] == 0xCE && 
                            buffer[i + j + 2] == 0xB0 && 
                            buffer[i + j + 3] == 0x0C)
                        {
                            return i + j;
                        }
                    }
                }
                
                i += 16;
            }
            
            // Check remaining bytes
            while (i < length)
            {
                if (buffer[i] == 0xFA && 
                    buffer[i + 1] == 0xCE && 
                    buffer[i + 2] == 0xB0 && 
                    buffer[i + 3] == 0x0C)
                {
                    return i;
                }
                i++;
            }
        }
        
        return -1;
    }
    
    /// <summary>
    /// Scalar sync marker scanning fallback.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ScanForSyncMarkerScalar(ReadOnlySpan<byte> buffer)
    {
        for (int i = 0; i <= buffer.Length - 4; i++)
        {
            if (buffer[i] == 0xFA && 
                buffer[i + 1] == 0xCE && 
                buffer[i + 2] == 0xB0 && 
                buffer[i + 3] == 0x0C)
            {
                return i;
            }
        }
        
        return -1;
    }
    
    /// <summary>
    /// Validates a frame header at the specified offset.
    /// </summary>
    /// <param name="offset">The offset to validate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A tuple containing whether the header is valid and the parsed header.</returns>
    public async Task<(bool IsValid, WalFrameHeader Header)> ValidateFrameHeaderAsync(long offset, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        if (offset < WalFileHeader.Size || offset + WalFrameHeader.Size > _fileSize)
        {
            return (false, default);
        }
        
        _fileStream.Position = offset;
        
        var buffer = new byte[WalFrameHeader.Size];
        var bytesRead = await _fileStream.ReadAsync(buffer, cancellationToken);
        
        if (bytesRead != WalFrameHeader.Size)
        {
            return (false, default);
        }
        
        var header = WalFrameHeader.ReadFrom(buffer);
        return (header.IsValid, header);
    }
    
    /// <summary>
    /// Reads the file header.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The file header.</returns>
    public async Task<WalFileHeader> ReadFileHeaderAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        
        _fileStream.Position = 0;
        
        var buffer = new byte[WalFileHeader.Size];
        await _fileStream.ReadExactlyAsync(buffer, cancellationToken);
        
        return WalFileHeader.ReadFrom(buffer);
    }
    
    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        
        _disposed = true;
        _fileStream.Dispose();
    }
}