using Lumina.Storage.Catalog;

using System.Globalization;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Compaction tier that merges daily L2 files into monthly L2 archives.
/// <para>
/// Groups by UTC calendar month.  A month is considered closed when
/// <c>DateTime.UtcNow.Date ≥ first day of the next month</c>.
/// </para>
/// <list type="table">
///   <item><term>Input</term><description>daily L2 files (CompactionTier 2)</description></item>
///   <item><term>Output</term><description>monthly L2 files (CompactionTier 3)</description></item>
///   <item><term>Filename</term><description><c>stream_yyyyMM.parquet</c></description></item>
/// </list>
/// </summary>
public sealed class MonthlyCompactionTier : ICompactionTier
{
  /// <inheritdoc />
  public int Order => 2;

  /// <inheritdoc />
  public string Name => "Monthly";

  /// <inheritdoc />
  public StorageLevel InputLevel => StorageLevel.L2;

  /// <inheritdoc />
  public int InputCompactionTier => 2;

  /// <inheritdoc />
  public int OutputCompactionTier => 3;

  /// <inheritdoc />
  public int MinGroupSize => 2;

  /// <inheritdoc />
  public IEnumerable<IGrouping<string, CatalogEntry>> GroupEntries(
      IReadOnlyList<CatalogEntry> entries)
  {
    return entries
        .GroupBy(e => $"{e.MinTime.Year}{e.MinTime.Month:D2}");
  }

  /// <inheritdoc />
  public bool IsGroupClosed(string groupKey)
  {
    var year = int.Parse(groupKey[..4], CultureInfo.InvariantCulture);
    var month = int.Parse(groupKey[4..], CultureInfo.InvariantCulture);
    var firstDayNextMonth = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1);
    return DateTime.UtcNow.Date >= firstDayNextMonth.Date;
  }

  /// <inheritdoc />
  public string GetOutputFileName(string stream, string groupKey)
      => $"{stream}_{groupKey}.parquet";
}
