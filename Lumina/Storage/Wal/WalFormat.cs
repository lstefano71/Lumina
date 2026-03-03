namespace Lumina.Storage.Wal;

/// <summary>
/// WAL file magic bytes: "LUMI" in ASCII
/// </summary>
public static class WalFormat
{
    /// <summary>
    /// Magic number for WAL file identification - "LUMI" in little-endian.
    /// </summary>
    public const uint Magic = 0x494D554C; // "LUMI" little-endian
    
    /// <summary>
    /// Current WAL format version.
    /// </summary>
    public const byte CurrentVersion = 0x01;
    
    /// <summary>
    /// Sync marker for frame boundary detection - 0xFA 0xCE 0xB0 0x0C in little-endian.
    /// Used for corruption recovery and frame alignment.
    /// </summary>
    public const uint SyncMarker = 0x0CB0CEFA; // 0xFA 0xCE 0xB0 0x0C little-endian
    
    /// <summary>
    /// Sync marker bytes for scanning purposes.
    /// </summary>
    public static ReadOnlySpan<byte> SyncMarkerBytes => new byte[] { 0xFA, 0xCE, 0xB0, 0x0C };
}