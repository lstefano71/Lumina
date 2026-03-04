using Lumina.Storage.Catalog;

using System.Globalization;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Compaction tier that merges L1 files into daily L2 archives.
/// <para>
/// Groups by UTC calendar day.  A day is considered closed when
/// <c>DateTime.UtcNow.Date &gt; day</c>.
/// </para>
/// <list type="table">
///   <item><term>Input</term><description>L1 files (CompactionTier 1)</description></item>
///   <item><term>Output</term><description>daily L2 files (CompactionTier 2)</description></item>
///   <item><term>Filename</term><description><c>stream_yyyyMMdd.parquet</c></description></item>
/// </list>
/// </summary>
public sealed class DailyCompactionTier : ICompactionTier
{
  /// <inheritdoc />
  public int Order => 1;

  /// <inheritdoc />
  public string Name => "Daily";

  /// <inheritdoc />
  public StorageLevel InputLevel => StorageLevel.L1;

  /// <inheritdoc />
  public int InputCompactionTier => 1;

  /// <inheritdoc />
  public int OutputCompactionTier => 2;

  /// <inheritdoc />
  public int MinGroupSize => 1;

  /// <inheritdoc />
  public IEnumerable<IGrouping<string, CatalogEntry>> GroupEntries(
      IReadOnlyList<CatalogEntry> entries)
  {
    return entries
        .GroupBy(e => e.MinTime.Date.ToString("yyyyMMdd", CultureInfo.InvariantCulture));
  }

  /// <inheritdoc />
  public bool IsGroupClosed(string groupKey)
  {
    var date = DateTime.ParseExact(
        groupKey, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
    return DateTime.UtcNow.Date > date;
  }

  /// <inheritdoc />
  public string GetOutputFileName(string stream, string groupKey)
      => $"{stream}_{groupKey}.parquet";
}
