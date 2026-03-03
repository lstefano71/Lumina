namespace Lumina.Storage.Compaction;

/// <summary>
/// CRC-32 implementation for cursor file payload validation.
/// Uses the CRC-32/ISO-HDLC polynomial (0x04C11DB7, reflected 0xEDB88320).
/// </summary>
public static class Crc32
{
  /// <summary>
  /// CRC-32 polynomial (reflected form): x^32 + x^26 + x^23 + x^22 + x^16 + x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1.
  /// </summary>
  private const uint Polynomial = 0xEDB88320;

  /// <summary>
  /// Precomputed CRC-32 lookup table for fast computation.
  /// </summary>
  private static readonly uint[] LookupTable = GenerateLookupTable();

  /// <summary>
  /// Computes the CRC-32 checksum of the provided data.
  /// </summary>
  /// <param name="data">The data to compute the checksum for.</param>
  /// <returns>The CRC-32 checksum.</returns>
  public static uint Compute(ReadOnlySpan<byte> data)
  {
    uint crc = 0xFFFFFFFF;

    for (int i = 0; i < data.Length; i++) {
      crc = (crc >> 8) ^ LookupTable[(crc ^ data[i]) & 0xFF];
    }

    return crc ^ 0xFFFFFFFF;
  }

  /// <summary>
  /// Computes the CRC-32 checksum of the provided data with an initial value.
  /// </summary>
  /// <param name="data">The data to compute the checksum for.</param>
  /// <param name="initial">The initial CRC value (for chaining).</param>
  /// <returns>The CRC-32 checksum.</returns>
  public static uint Compute(ReadOnlySpan<byte> data, uint initial)
  {
    uint crc = initial ^ 0xFFFFFFFF;

    for (int i = 0; i < data.Length; i++) {
      crc = (crc >> 8) ^ LookupTable[(crc ^ data[i]) & 0xFF];
    }

    return crc ^ 0xFFFFFFFF;
  }

  /// <summary>
  /// Validates data against an expected CRC-32 value.
  /// </summary>
  /// <param name="data">The data to validate.</param>
  /// <param name="expectedCrc">The expected CRC-32 checksum.</param>
  /// <returns>True if the checksum matches; otherwise false.</returns>
  public static bool Validate(ReadOnlySpan<byte> data, uint expectedCrc)
  {
    return Compute(data) == expectedCrc;
  }

  /// <summary>
  /// Generates the CRC-32 lookup table for fast computation.
  /// Uses the reflected polynomial for byte-at-a-time processing.
  /// </summary>
  private static uint[] GenerateLookupTable()
  {
    var table = new uint[256];

    for (uint i = 0; i < 256; i++) {
      uint crc = i;

      for (int bit = 0; bit < 8; bit++) {
        if ((crc & 1) != 0) {
          crc = (crc >> 1) ^ Polynomial;
        } else {
          crc >>= 1;
        }
      }

      table[i] = crc;
    }

    return table;
  }
}