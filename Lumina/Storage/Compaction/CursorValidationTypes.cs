namespace Lumina.Storage.Compaction;

/// <summary>
/// Result of cursor validation.
/// </summary>
public enum CursorValidationResult
{
  /// <summary>
  /// Cursor is valid and passed all checks.
  /// </summary>
  Valid,

  /// <summary>
  /// Cursor file not found, new cursor will be created.
  /// </summary>
  NotFound,

  /// <summary>
  /// Magic number mismatch - not a valid cursor file.
  /// </summary>
  InvalidMagic,

  /// <summary>
  /// Version mismatch - unsupported format version.
  /// </summary>
  UnsupportedVersion,

  /// <summary>
  /// CRC checksum mismatch - file is corrupted.
  /// </summary>
  ChecksumMismatch,

  /// <summary>
  /// Payload length is invalid or truncated file.
  /// </summary>
  TruncatedPayload,

  /// <summary>
  /// JSON deserialization failed.
  /// </summary>
  InvalidJson,

  /// <summary>
  /// Referenced WAL file no longer exists.
  /// </summary>
  WalFileNotFound,

  /// <summary>
  /// Referenced Parquet file no longer exists.
  /// </summary>
  ParquetFileNotFound
}

/// <summary>
/// Contains information about cursor recovery operations.
/// </summary>
public sealed class CursorRecoveryInfo
{
  /// <summary>
  /// Gets the stream name.
  /// </summary>
  public required string Stream { get; init; }

  /// <summary>
  /// Gets the validation result for the original cursor.
  /// </summary>
  public CursorValidationResult ValidationResult { get; init; }

  /// <summary>
  /// Gets a value indicating whether recovery was performed.
  /// </summary>
  public bool WasRecovered { get; init; }

  /// <summary>
  /// Gets the recovery method used, if any.
  /// </summary>
  public string? RecoveryMethod { get; init; }

  /// <summary>
  /// Gets the original cursor offset before recovery.
  /// </summary>
  public long OriginalOffset { get; init; }

  /// <summary>
  /// Gets the recovered cursor offset after recovery.
  /// </summary>
  public long RecoveredOffset { get; init; }

  /// <summary>
  /// Gets the timestamp of the recovery operation.
  /// </summary>
  public DateTime RecoveryTimestamp { get; init; }

  /// <summary>
  /// Gets the exception that occurred during validation, if any.
  /// </summary>
  public Exception? Exception { get; init; }
}