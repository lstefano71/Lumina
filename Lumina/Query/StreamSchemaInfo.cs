namespace Lumina.Query;

/// <summary>
/// Information about a result column.
/// </summary>
public sealed class ColumnInfo
{
  /// <summary>
  /// Gets the column name.
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  /// Gets the column data type.
  /// </summary>
  public required string Type { get; init; }

  /// <summary>
  /// Gets a value indicating whether the column can contain null values.
  /// </summary>
  public bool IsNullable { get; init; } = true;
}

/// <summary>
/// Schema information for a stream.
/// </summary>
public sealed class StreamSchemaInfo
{
  /// <summary>
  /// Gets the stream name.
  /// </summary>
  public required string StreamName { get; init; }

  /// <summary>
  /// Gets the columns in the stream schema.
  /// </summary>
  public required IReadOnlyList<ColumnInfo> Columns { get; init; }

  /// <summary>
  /// Gets the number of Parquet files backing this stream.
  /// </summary>
  public int FileCount { get; init; }

  /// <summary>
  /// Gets the total size in bytes of all Parquet files.
  /// </summary>
  public long TotalSizeBytes { get; init; }

  /// <summary>
  /// Gets the earliest timestamp in the stream.
  /// </summary>
  public DateTime? MinTimestamp { get; init; }

  /// <summary>
  /// Gets the latest timestamp in the stream.
  /// </summary>
  public DateTime? MaxTimestamp { get; init; }
}