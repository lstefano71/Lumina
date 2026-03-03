using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Cursor file header with integrity validation.
/// Written at the start of each cursor file for format identification and checksum verification.
/// Total size: 16 bytes.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct CursorFileHeader
{
  /// <summary>
  /// Magic number for cursor file identification - "LCUR" in little-endian (4 bytes).
  /// Value: 0x52434C4C (bytes: 0x4C 0x43 0x52 0x4C = "LCUR").
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
  /// Reserved byte for future use.
  /// </summary>
  public readonly byte Reserved1;

  /// <summary>
  /// Reserved byte for future use.
  /// </summary>
  public readonly byte Reserved2;

  /// <summary>
  /// Reserved byte for future use.
  /// </summary>
  public readonly byte Reserved3;

  /// <summary>
  /// CRC-32 checksum of the JSON payload (4 bytes).
  /// Uses CRC-32/ISO-HDLC polynomial.
  /// </summary>
  public readonly uint PayloadChecksum;

  /// <summary>
  /// Length of the JSON payload in bytes (4 bytes).
  /// </summary>
  public readonly uint PayloadLength;

  /// <summary>
  /// Initializes a new instance of the CursorFileHeader struct.
  /// </summary>
  /// <param name="payloadChecksum">The CRC-32 checksum of the payload.</param>
  /// <param name="payloadLength">The length of the payload in bytes.</param>
  public CursorFileHeader(uint payloadChecksum, uint payloadLength)
  {
    Magic = ExpectedMagic;
    Version = CurrentVersion;
    Reserved1 = 0;
    Reserved2 = 0;
    Reserved3 = 0;
    PayloadChecksum = payloadChecksum;
    PayloadLength = payloadLength;
  }

  /// <summary>
  /// Gets a value indicating whether this header has a valid magic number.
  /// </summary>
  public bool HasValidMagic => Magic == ExpectedMagic;

  /// <summary>
  /// Gets a value indicating whether this header has a supported version.
  /// </summary>
  public bool HasSupportedVersion => Version == CurrentVersion;

  /// <summary>
  /// Gets a value indicating whether this header passes basic validation.
  /// </summary>
  public bool IsValid => HasValidMagic && HasSupportedVersion;

  /// <summary>
  /// Writes the header to a span.
  /// Uses explicit little-endian encoding for cross-platform consistency.
  /// </summary>
  /// <param name="destination">The destination span (must be at least Size bytes).</param>
  public void WriteTo(Span<byte> destination)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, Size);

    int offset = 0;

    // Magic (4 bytes, little-endian)
    BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset, 4), Magic);
    offset += 4;

    // Version (1 byte)
    destination[offset++] = Version;

    // Reserved (3 bytes)
    destination[offset++] = Reserved1;
    destination[offset++] = Reserved2;
    destination[offset++] = Reserved3;

    // PayloadChecksum (4 bytes, little-endian)
    BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset, 4), PayloadChecksum);
    offset += 4;

    // PayloadLength (4 bytes, little-endian)
    BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset, 4), PayloadLength);
  }

  /// <summary>
  /// Reads a header from a span.
  /// Uses explicit little-endian encoding for cross-platform consistency.
  /// </summary>
  /// <param name="source">The source span (must be at least Size bytes).</param>
  /// <returns>The parsed CursorFileHeader.</returns>
  public static CursorFileHeader ReadFrom(ReadOnlySpan<byte> source)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(source.Length, Size);

    int offset = 0;

    // Magic (4 bytes, little-endian)
    uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(offset, 4));
    offset += 4;

    // Version (1 byte)
    byte version = source[offset++];

    // Reserved (3 bytes)
    byte reserved1 = source[offset++];
    byte reserved2 = source[offset++];
    byte reserved3 = source[offset++];

    // PayloadChecksum (4 bytes, little-endian)
    uint payloadChecksum = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(offset, 4));
    offset += 4;

    // PayloadLength (4 bytes, little-endian)
    uint payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(offset, 4));

    // Reconstruct struct via reflection since fields are readonly
    var header = (CursorFileHeader)RuntimeHelpers.GetUninitializedObject(typeof(CursorFileHeader));

    // Use reflection to set readonly fields
    typeof(CursorFileHeader).GetField(nameof(Magic))!.SetValueDirect(__makeref(header), magic);
    typeof(CursorFileHeader).GetField(nameof(Version))!.SetValueDirect(__makeref(header), version);
    typeof(CursorFileHeader).GetField(nameof(Reserved1))!.SetValueDirect(__makeref(header), reserved1);
    typeof(CursorFileHeader).GetField(nameof(Reserved2))!.SetValueDirect(__makeref(header), reserved2);
    typeof(CursorFileHeader).GetField(nameof(Reserved3))!.SetValueDirect(__makeref(header), reserved3);
    typeof(CursorFileHeader).GetField(nameof(PayloadChecksum))!.SetValueDirect(__makeref(header), payloadChecksum);
    typeof(CursorFileHeader).GetField(nameof(PayloadLength))!.SetValueDirect(__makeref(header), payloadLength);

    return header;
  }

  /// <summary>
  /// Creates a header for the given payload.
  /// </summary>
  /// <param name="payload">The payload bytes.</param>
  /// <returns>A new CursorFileHeader with computed checksum.</returns>
  public static CursorFileHeader CreateForPayload(ReadOnlySpan<byte> payload)
  {
    uint checksum = Crc32.Compute(payload);
    return new CursorFileHeader(checksum, (uint)payload.Length);
  }

  /// <summary>
  /// Validates the header against the provided payload.
  /// </summary>
  /// <param name="payload">The payload to validate.</param>
  /// <returns>True if the payload matches the header's checksum and length.</returns>
  public bool ValidatePayload(ReadOnlySpan<byte> payload)
  {
    if (payload.Length != PayloadLength) {
      return false;
    }

    uint computedChecksum = Crc32.Compute(payload);
    return computedChecksum == PayloadChecksum;
  }

  /// <summary>
  /// Attempts to read and validate a header from the given span.
  /// </summary>
  /// <param name="source">The source buffer.</param>
  /// <param name="header">The parsed header if valid.</param>
  /// <returns>True if the header could be read; otherwise false.</returns>
  public static bool TryRead(ReadOnlySpan<byte> source, out CursorFileHeader header)
  {
    if (source.Length < Size) {
      header = default;
      return false;
    }

    header = ReadFrom(source);
    return true;
  }
}