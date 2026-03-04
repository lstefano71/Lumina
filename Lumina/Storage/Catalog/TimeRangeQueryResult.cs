namespace Lumina.Storage.Catalog;

/// <summary>
/// Result of a time-range catalog query.
/// </summary>
public sealed class TimeRangeQueryResult
{
  /// <summary>
  /// Gets the catalog entries that overlap with the query time range.
  /// </summary>
  public required IReadOnlyList<CatalogEntry> Entries { get; init; }

  /// <summary>
  /// Gets the total number of files matching the query.
  /// </summary>
  public int TotalFiles { get; init; }

  /// <summary>
  /// Gets the total number of rows across all matching files.
  /// </summary>
  public long TotalRows { get; init; }

  /// <summary>
  /// Gets the global minimum timestamp across all matching files.
  /// </summary>
  public DateTime? GlobalMinTime { get; init; }

  /// <summary>
  /// Gets the global maximum timestamp across all matching files.
  /// </summary>
  public DateTime? GlobalMaxTime { get; init; }
}