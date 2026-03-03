using System.Runtime.InteropServices;

namespace Lumina.Storage.Wal;

/// <summary>
/// WAL file header - 8 bytes total.
/// Written at the beginning of each WAL file for format identification.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct WalFileHeader
{
  /// <summary>
  /// Magic number for file identification - "LUMI" in little-endian (4 bytes).
  /// </summary>
  public readonly uint Magic;

  /// <summary>
  /// Format version number (1 byte).
  /// </summary>
  public readonly byte Version;

  /// <summary>
  /// Flags for compression, encryption, etc. (1 byte).
  /// Currently reserved for future use.
  /// </summary>
  public readonly byte Flags;

  /// <summary>
  /// Reserved for future use (2 bytes).
  /// </summary>
  public readonly ushort Reserved;

  /// <summary>
  /// Total size of the file header in bytes.
  /// </summary>
  public const int Size = 8;

  /// <summary>
  /// Expected magic value for valid WAL files.
  /// </summary>
  public const uint ExpectedMagic = 0x494D554C; // "LUMI"

  /// <summary>
  /// Initializes a new instance of the WalFileHeader struct.
  /// </summary>
  /// <param name="version">The format version.</param>
  /// <param name="flags">Optional flags for compression/encryption.</param>
  public WalFileHeader(byte version = WalFormat.CurrentVersion, byte flags = 0)
  {
    Magic = ExpectedMagic;
    Version = version;
    Flags = flags;
    Reserved = 0;
  }

  /// <summary>
  /// Gets a value indicating whether this header is valid.
  /// </summary>
  public bool IsValid => Magic == ExpectedMagic && Version == WalFormat.CurrentVersion;

  /// <summary>
  /// Writes the header to a span.
  /// </summary>
  /// <param name="destination">The destination span (must be at least Size bytes).</param>
  public void WriteTo(Span<byte> destination)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(destination.Length, Size);

    destination[0] = (byte)(Magic & 0xFF);
    destination[1] = (byte)((Magic >> 8) & 0xFF);
    destination[2] = (byte)((Magic >> 16) & 0xFF);
    destination[3] = (byte)((Magic >> 24) & 0xFF);
    destination[4] = Version;
    destination[5] = Flags;
    destination[6] = (byte)(Reserved & 0xFF);
    destination[7] = (byte)((Reserved >> 8) & 0xFF);
  }

  /// <summary>
  /// Reads a header from a span.
  /// </summary>
  /// <param name="source">The source span (must be at least Size bytes).</param>
  /// <returns>The parsed WalFileHeader.</returns>
  public static WalFileHeader ReadFrom(ReadOnlySpan<byte> source)
  {
    ArgumentOutOfRangeException.ThrowIfLessThan(source.Length, Size);

    // Use MemoryMarshal to directly reinterpret the bytes as the struct
    return MemoryMarshal.Read<WalFileHeader>(source);
  }

  /// <summary>
  /// Creates a default header with current version.
  /// </summary>
  public static WalFileHeader CreateDefault() => new(WalFormat.CurrentVersion);
}