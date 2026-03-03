using System.Runtime.InteropServices;

namespace Lumina.Storage.Wal;

/// <summary>
/// WAL entry frame header - 14 bytes before payload.
/// Provides frame boundary detection and basic error correction.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct WalFrameHeader
{
  /// <summary>
  /// Sync marker for frame boundary detection (4 bytes).
  /// Value: 0x0CB0CEFA (0xFA 0xCE 0xB0 0x0C in byte order).
  /// </summary>
  public readonly uint SyncMarker;

  /// <summary>
  /// Length of the payload in bytes (4 bytes).
  /// </summary>
  public readonly uint Length;

  /// <summary>
  /// Bitwise NOT of Length for forward error correction (4 bytes).
  /// Used to detect corrupted length values.
  /// </summary>
  public readonly uint InvertedLength;

  /// <summary>
  /// Type of the entry (1 byte).
  /// </summary>
  public readonly WalEntryType Type;

  /// <summary>
  /// CRC-8 checksum of Length, InvertedLength, and Type (1 byte).
  /// </summary>
  public readonly byte HeaderCrc;

  /// <summary>
  /// Total size of the frame header in bytes.
  /// </summary>
  public const int Size = 14;

  /// <summary>
  /// Initializes a new instance of the WalFrameHeader struct.
  /// </summary>
  /// <param name="length">The payload length.</param>
  /// <param name="type">The entry type.</param>
  public WalFrameHeader(uint length, WalEntryType type)
  {
    SyncMarker = WalFormat.SyncMarker;
    Length = length;
    InvertedLength = ~length;
    Type = type;
    HeaderCrc = ComputeCrc(length, type);
  }

  /// <summary>
  /// Gets a value indicating whether this header passes basic validation.
  /// </summary>
  public bool IsValid => SyncMarker == WalFormat.SyncMarker &&
                        Length == ~InvertedLength &&
                        HeaderCrc == ComputeCrc(Length, Type);

  /// <summary>
  /// Gets the end-of-frame offset (SyncMarker + Length).
  /// </summary>
  public long EndOffset => Size + Length;

  /// <summary>
  /// Computes the CRC-8 for the header fields.
  /// </summary>
  private static byte ComputeCrc(uint length, WalEntryType type)
  {
    Span<byte> data = stackalloc byte[9];

    // Length (4 bytes, little-endian)
    data[0] = (byte)(length & 0xFF);
    data[1] = (byte)((length >> 8) & 0xFF);
    data[2] = (byte)((length >> 16) & 0xFF);
    data[3] = (byte)((length >> 24) & 0xFF);

    // InvertedLength (4 bytes, little-endian) - included for extra validation
    uint inverted = ~length;
    data[4] = (byte)(inverted & 0xFF);
    data[5] = (byte)((inverted >> 8) & 0xFF);
    data[6] = (byte)((inverted >> 16) & 0xFF);
    data[7] = (byte)((inverted >> 24) & 0xFF);

    // Type (1 byte)
    data[8] = (byte)type;

    return Crc8.Compute(data);
  }

  /// <summary>
  /// Writes the header to a span.
  /// </summary>
  /// <param name="destination">The destination span (must be at least Size bytes).</param>
  public void WriteTo(Span<byte> destination)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, Size);

    int offset = 0;

    // SyncMarker (4 bytes, little-endian)
    destination[offset++] = (byte)(SyncMarker & 0xFF);
    destination[offset++] = (byte)((SyncMarker >> 8) & 0xFF);
    destination[offset++] = (byte)((SyncMarker >> 16) & 0xFF);
    destination[offset++] = (byte)((SyncMarker >> 24) & 0xFF);

    // Length (4 bytes, little-endian)
    destination[offset++] = (byte)(Length & 0xFF);
    destination[offset++] = (byte)((Length >> 8) & 0xFF);
    destination[offset++] = (byte)((Length >> 16) & 0xFF);
    destination[offset++] = (byte)((Length >> 24) & 0xFF);

    // InvertedLength (4 bytes, little-endian)
    destination[offset++] = (byte)(InvertedLength & 0xFF);
    destination[offset++] = (byte)((InvertedLength >> 8) & 0xFF);
    destination[offset++] = (byte)((InvertedLength >> 16) & 0xFF);
    destination[offset++] = (byte)((InvertedLength >> 24) & 0xFF);

    // Type (1 byte)
    destination[offset++] = (byte)Type;

    // HeaderCrc (1 byte)
    destination[offset] = HeaderCrc;
  }

  /// <summary>
  /// Reads a header from a span.
  /// </summary>
  /// <param name="source">The source span (must be at least Size bytes).</param>
  /// <returns>The parsed WalFrameHeader.</returns>
  public static WalFrameHeader ReadFrom(ReadOnlySpan<byte> source)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(source.Length, Size);

    return MemoryMarshal.Read<WalFrameHeader>(source);
  }

  /// <summary>
  /// Attempts to validate a frame header at the given offset.
  /// </summary>
  /// <param name="source">The source buffer.</param>
  /// <param name="header">The parsed header if valid.</param>
  /// <returns>True if the header is valid; otherwise false.</returns>
  public static bool TryValidate(ReadOnlySpan<byte> source, out WalFrameHeader header)
  {
    if (source.Length < Size) {
      header = default;
      return false;
    }

    header = ReadFrom(source);
    return header.IsValid;
  }
}