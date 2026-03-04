using Lumina.Storage.Catalog;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Defines the strategy for a single calendar-based compaction tier.
/// <para>
/// Each tier describes <em>what varies</em> — eligible source entries,
/// how they are grouped by calendar boundary, when a group is considered
/// closed, and the output naming / tier metadata.  The shared merge
/// logic (read → write → catalog → delete) lives in <see cref="CompactionPipeline"/>.
/// </para>
/// <para>
/// To add a new tier (e.g., weekly, quarterly, yearly) implement this
/// interface and register it in the DI container.
/// </para>
/// </summary>
public interface ICompactionTier
{
  /// <summary>
  /// Execution priority — lower values run first.
  /// Daily = 1, Monthly = 2, Yearly = 3, etc.
  /// Earlier tiers produce files that feed later tiers in the same cycle.
  /// </summary>
  int Order { get; }

  /// <summary>
  /// Display name used in log messages (e.g., "Daily", "Monthly").
  /// </summary>
  string Name { get; }

  /// <summary>
  /// The <see cref="StorageLevel"/> of source entries consumed by this tier.
  /// </summary>
  StorageLevel InputLevel { get; }

  /// <summary>
  /// The <see cref="CatalogEntry.CompactionTier"/> value of source entries.
  /// </summary>
  int InputCompactionTier { get; }

  /// <summary>
  /// The <see cref="CatalogEntry.CompactionTier"/> value written to the output entry.
  /// </summary>
  int OutputCompactionTier { get; }

  /// <summary>
  /// Minimum number of entries in a group before merging is worthwhile.
  /// Daily = 1 (always promote), Monthly = 2 (skip single-file months).
  /// </summary>
  int MinGroupSize { get; }

  /// <summary>
  /// Groups eligible catalog entries by their calendar boundary.
  /// The <c>string</c> key is a short label such as <c>"20240110"</c>
  /// (daily) or <c>"202401"</c> (monthly) — it is used for both
  /// logging and filename generation via <see cref="GetOutputFileName"/>.
  /// </summary>
  IEnumerable<IGrouping<string, CatalogEntry>> GroupEntries(
      IReadOnlyList<CatalogEntry> entries);

  /// <summary>
  /// Returns <c>true</c> when the calendar window identified by
  /// <paramref name="groupKey"/> is fully closed (i.e., no more data
  /// can arrive for that window).
  /// </summary>
  bool IsGroupClosed(string groupKey);

  /// <summary>
  /// Builds the output Parquet filename (without directory) for a group.
  /// Example: <c>"my-stream_20240110.parquet"</c>.
  /// </summary>
  string GetOutputFileName(string stream, string groupKey);
}
