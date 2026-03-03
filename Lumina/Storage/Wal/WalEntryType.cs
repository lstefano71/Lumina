namespace Lumina.Storage.Wal;

/// <summary>
/// Entry types in the WAL frame.
/// </summary>
public enum WalEntryType : byte
{
    /// <summary>
    /// Standard log entry.
    /// </summary>
    StandardLog = 0x01,
    
    /// <summary>
    /// Metric data point.
    /// </summary>
    Metric = 0x02,
    
    /// <summary>
    /// Trace span.
    /// </summary>
    Trace = 0x03
}