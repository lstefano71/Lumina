using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;

namespace Lumina.Storage.Compaction;

/// <summary>
/// N-tier calendar-based compaction engine.
///   Phase 1 (Daily):   L1 files → daily L2 files   (stream_yyyyMMdd.parquet)
///   Phase 2 (Monthly): daily L2 → monthly L2 files  (stream_yyyyMM.parquet)
/// Uses catalog time-range queries and Parquet metadata — never parses filenames.
/// At startup the first run catches up on every missed calendar window automatically.
/// </summary>
public sealed class L2Compactor
{
  private readonly CompactionSettings _settings;
  private readonly ParquetManager _parquetManager;
  private readonly CatalogManager? _catalogManager;
  private readonly ILogger<L2Compactor> _logger;

  public L2Compactor(
      CompactionSettings settings,
      ParquetManager parquetManager,
      ILogger<L2Compactor> logger,
      CatalogManager? catalogManager = null)
  {
    _settings = settings;
    _parquetManager = parquetManager;
    _logger = logger;
    _catalogManager = catalogManager;
  }

  // ---------------------------------------------------------------------------
  //  Public entry point
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Runs all compaction tiers for every stream.
  /// Phase 1 (Daily) runs first so that freshly created daily files are
  /// immediately available for Phase 2 (Monthly) in the same cycle.
  /// </summary>
  /// <returns>Total number of source files consumed across all tiers.</returns>
  public async Task<int> CompactAllAsync(CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) {
      _logger.LogWarning("L2 compaction requires CatalogManager");
      return 0;
    }

    var streams = _catalogManager.GetStreams();
    var totalCompacted = 0;

    foreach (var stream in streams) {
      if (cancellationToken.IsCancellationRequested) break;

      try {
        // Phase 1 — Daily
        var dailyCount = await CompactStreamDailyAsync(stream, cancellationToken);
        totalCompacted += dailyCount;

        // Phase 2 — Monthly (runs after daily so new daily files are available)
        var monthlyCount = await CompactStreamMonthlyAsync(stream, cancellationToken);
        totalCompacted += monthlyCount;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to compact stream {Stream}", stream);
      }
    }

    return totalCompacted;
  }

  // ---------------------------------------------------------------------------
  //  Phase 1 — Daily compaction  (L1 → daily L2)
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Compacts L1 files whose data belongs entirely to a closed UTC day
  /// into a single daily L2 file per day.
  /// A day is considered closed when <c>UtcNow.Date</c> &gt; the day in question.
  /// </summary>
  public async Task<int> CompactStreamDailyAsync(
      string stream, CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) return 0;

    // cutoff = start of today → any L1 file whose MaxTime < today is fair game
    var cutoffDate = DateTime.UtcNow.Date;
    var eligibleEntries = _catalogManager.GetEligibleForDailyCompaction(stream, cutoffDate);

    if (eligibleEntries.Count == 0) return 0;

    // Group by the calendar day the data starts on
    var groupsByDate = eligibleEntries
        .GroupBy(e => e.MinTime.Date)
        .ToList();

    var consolidatedCount = 0;

    foreach (var group in groupsByDate) {
      if (cancellationToken.IsCancellationRequested) break;

      var entries = group.ToList();
      var date = group.Key;

      try {
        await ConsolidateDayAsync(stream, date, entries, cancellationToken);
        consolidatedCount += entries.Count;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed daily compaction for {Stream}/{Date}", stream, date);
      }
    }

    return consolidatedCount;
  }

  /// <summary>
  /// Merges the given L1 catalog entries into a single daily L2 Parquet file.
  /// Atomic commit order: write file → update catalog → delete originals.
  /// </summary>
  private async Task ConsolidateDayAsync(
      string stream,
      DateTime date,
      IReadOnlyList<CatalogEntry> l1Entries,
      CancellationToken cancellationToken)
  {
    var entries = new List<LogEntry>();
    var sourceFiles = l1Entries.Select(e => e.FilePath).ToList();

    foreach (var file in sourceFiles) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file, cancellationToken)) {
        entries.Add(entry);
      }
    }

    if (entries.Count == 0) {
      _logger.LogDebug("No entries to consolidate for {Stream} on {Date}", stream, date);
      return;
    }

    var minTime = entries.Min(e => e.Timestamp);
    var maxTime = entries.Max(e => e.Timestamp);

    // Output: stream_yyyyMMdd.parquet
    var outputDir = Path.Combine(_settings.L2Directory, stream);
    var outputFileName = $"{stream}_{date:yyyyMMdd}.parquet";
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));
    var tmpOutputPath = outputPath + ".tmp";

    await ParquetWriter.WriteBatchAsync(entries, tmpOutputPath, _settings.MaxDynamicKeys, cancellationToken);
    File.Move(tmpOutputPath, outputPath, overwrite: true);

    var fileInfo = new FileInfo(outputPath);
    var l2Entry = new CatalogEntry {
      StreamName = stream,
      MinTime = minTime,
      MaxTime = maxTime,
      FilePath = outputPath,
      Level = StorageLevel.L2,
      RowCount = entries.Count,
      FileSizeBytes = fileInfo.Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 2
    };

    await _catalogManager!.ReplaceFilesAsync(sourceFiles, l2Entry, cancellationToken);

    _logger.LogInformation(
        "Daily compaction: {Count} entries from {Files} L1 files → {Output}",
        entries.Count, sourceFiles.Count, outputFileName);

    DeleteSourceFiles(sourceFiles);
  }

  // ---------------------------------------------------------------------------
  //  Phase 2 — Monthly compaction  (daily L2 → monthly L2)
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Compacts daily L2 files into a single monthly L2 file for each
  /// fully closed calendar month.
  /// A month is closed when <c>UtcNow.Date ≥ first day of the next month</c>.
  /// </summary>
  public async Task<int> CompactStreamMonthlyAsync(
      string stream, CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) return 0;

    // Find all daily-tier (CompactionTier == 2) L2 files for this stream
    var dailyEntries = _catalogManager
        .GetEligibleForMonthlyCompaction(stream);

    if (dailyEntries.Count == 0) return 0;

    // Group by (Year, Month)
    var monthGroups = dailyEntries
        .GroupBy(e => new { e.MinTime.Year, e.MinTime.Month })
        .ToList();

    var consolidatedCount = 0;

    foreach (var group in monthGroups) {
      if (cancellationToken.IsCancellationRequested) break;

      var year = group.Key.Year;
      var month = group.Key.Month;

      // Month must be fully closed
      var firstDayNextMonth = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1);
      if (firstDayNextMonth > DateTime.UtcNow.Date) continue;

      var entries = group.ToList();
      if (entries.Count < 2) continue; // nothing to gain from one file

      try {
        await ConsolidateMonthAsync(stream, year, month, entries, cancellationToken);
        consolidatedCount += entries.Count;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed monthly compaction for {Stream}/{Year}-{Month:D2}",
            stream, year, month);
      }
    }

    return consolidatedCount;
  }

  /// <summary>
  /// Merges the given daily L2 catalog entries into a single monthly L2 Parquet file.
  /// Atomic commit order: write file → update catalog → delete originals.
  /// </summary>
  private async Task ConsolidateMonthAsync(
      string stream,
      int year,
      int month,
      IReadOnlyList<CatalogEntry> dailyEntries,
      CancellationToken cancellationToken)
  {
    var entries = new List<LogEntry>();
    var sourceFiles = dailyEntries.Select(e => e.FilePath).ToList();

    foreach (var file in sourceFiles) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file, cancellationToken)) {
        entries.Add(entry);
      }
    }

    if (entries.Count == 0) {
      _logger.LogDebug("No entries for monthly compaction {Stream}/{Year}-{Month:D2}",
          stream, year, month);
      return;
    }

    var minTime = entries.Min(e => e.Timestamp);
    var maxTime = entries.Max(e => e.Timestamp);

    // Output: stream_yyyyMM.parquet
    var outputDir = Path.Combine(_settings.L2Directory, stream);
    var outputFileName = $"{stream}_{year}{month:D2}.parquet";
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));
    var tmpOutputPath = outputPath + ".tmp";

    await ParquetWriter.WriteBatchAsync(entries, tmpOutputPath, _settings.MaxDynamicKeys, cancellationToken);
    File.Move(tmpOutputPath, outputPath, overwrite: true);

    var fileInfo = new FileInfo(outputPath);
    var monthlyEntry = new CatalogEntry {
      StreamName = stream,
      MinTime = minTime,
      MaxTime = maxTime,
      FilePath = outputPath,
      Level = StorageLevel.L2,
      RowCount = entries.Count,
      FileSizeBytes = fileInfo.Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 3
    };

    await _catalogManager!.ReplaceFilesAsync(sourceFiles, monthlyEntry, cancellationToken);

    _logger.LogInformation(
        "Monthly compaction: {Count} entries from {Files} daily files → {Output}",
        entries.Count, sourceFiles.Count, outputFileName);

    DeleteSourceFiles(sourceFiles);
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Best-effort deletion of source files after a successful catalog commit.
  /// </summary>
  private void DeleteSourceFiles(IReadOnlyList<string> files)
  {
    foreach (var file in files) {
      try {
        File.Delete(file);
        _logger.LogDebug("Deleted source file: {File}", file);
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to delete source file: {File}", file);
      }
    }
  }
}