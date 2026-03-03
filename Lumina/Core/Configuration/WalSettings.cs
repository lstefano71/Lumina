namespace Lumina.Core.Configuration;

/// <summary>
/// Configuration settings for the Write-Ahead Log storage.
/// </summary>
public sealed class WalSettings
{
  /// <summary>
  /// Gets the directory where WAL files are stored.
  /// Default is "data".
  /// </summary>
  public string DataDirectory { get; init; } = "data";

  /// <summary>
  /// Gets the maximum size in bytes before a WAL file is rotated.
  /// Default is 100 MB.
  /// </summary>
  public long MaxWalSizeBytes { get; init; } = 100 * 1024 * 1024;

  /// <summary>
  /// Gets a value indicating whether write-through is enabled.
  /// When enabled, writes bypass the OS cache and go directly to disk.
  /// Default is true for durability.
  /// </summary>
  public bool EnableWriteThrough { get; init; } = true;

  /// <summary>
  /// Gets the interval in milliseconds between automatic flush operations.
  /// Default is 100ms.
  /// </summary>
  public int FlushIntervalMs { get; init; } = 100;
}