namespace Lumina.Storage.Wal;

/// <summary>
/// CRC-8 implementation for WAL frame header validation.
/// Uses the CRC-8/MAXIM polynomial (0x31) for efficient checksum computation.
/// </summary>
public static class Crc8
{
    /// <summary>
    /// CRC-8 polynomial: x^8 + x^5 + x^4 + 1 (0x31, also known as Dallas/Maxim).
    /// </summary>
    private const byte Polynomial = 0x31;
    
    /// <summary>
    /// Precomputed CRC-8 lookup table for fast computation.
    /// </summary>
    private static readonly byte[] LookupTable = GenerateLookupTable();
    
    /// <summary>
    /// Computes the CRC-8 checksum of the provided data.
    /// </summary>
    /// <param name="data">The data to compute the checksum for.</param>
    /// <returns>The CRC-8 checksum byte.</returns>
    public static byte Compute(ReadOnlySpan<byte> data)
    {
        byte crc = 0;
        
        for (int i = 0; i < data.Length; i++)
        {
            crc = LookupTable[crc ^ data[i]];
        }
        
        return crc;
    }
    
    /// <summary>
    /// Computes the CRC-8 checksum of the provided data with an initial value.
    /// </summary>
    /// <param name="data">The data to compute the checksum for.</param>
    /// <param name="initial">The initial CRC value (for chaining).</param>
    /// <returns>The CRC-8 checksum byte.</returns>
    public static byte Compute(ReadOnlySpan<byte> data, byte initial)
    {
        byte crc = initial;
        
        for (int i = 0; i < data.Length; i++)
        {
            crc = LookupTable[crc ^ data[i]];
        }
        
        return crc;
    }
    
    /// <summary>
    /// Validates data against an expected CRC-8 value.
    /// </summary>
    /// <param name="data">The data to validate.</param>
    /// <param name="expectedCrc">The expected CRC-8 checksum.</param>
    /// <returns>True if the checksum matches; otherwise false.</returns>
    public static bool Validate(ReadOnlySpan<byte> data, byte expectedCrc)
    {
        return Compute(data) == expectedCrc;
    }
    
    /// <summary>
    /// Generates the CRC-8 lookup table for fast computation.
    /// </summary>
    private static byte[] GenerateLookupTable()
    {
        var table = new byte[256];
        
        for (int i = 0; i < 256; i++)
        {
            byte crc = (byte)i;
            
            for (int bit = 0; bit < 8; bit++)
            {
                if ((crc & 0x80) != 0)
                {
                    crc = (byte)((crc << 1) ^ Polynomial);
                }
                else
                {
                    crc <<= 1;
                }
            }
            
            table[i] = crc;
        }
        
        return table;
    }
}