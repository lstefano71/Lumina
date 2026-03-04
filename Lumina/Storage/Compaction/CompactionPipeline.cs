using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;

namespace Lumina.Storage.Compaction;

/// <summary>
/// N-tier calendar-based compaction engine driven by <see cref="ICompactionTier"/> plugins.
/// <para>
/// Each registered tier defines eligibility, grouping, closedness, naming, and
/// output metadata.  The pipeline iterates tiers in <see cref="ICompactionTier.Order"/>
/// so that earlier tiers (e.g., Daily) produce files that feed later tiers
/// (e.g., Monthly) within the same cycle.
/// </para>
/// <para>
/// The shared merge logic — read sources → write merged Parquet → atomic catalog
/// commit → delete originals — lives here in <see cref="ConsolidateGroupAsync"/>.
/// </para>
/// </summary>
public sealed class CompactionPipeline
{
  private readonly CompactionSettings _settings;
  private readonly CatalogManager? _catalogManager;
  private readonly IReadOnlyList<ICompactionTier> _tiers;
  private readonly ILogger<CompactionPipeline> _logger;

  public CompactionPipeline(
      CompactionSettings settings,
      CatalogManager? catalogManager,
      IEnumerable<ICompactionTier> tiers,
      ILogger<CompactionPipeline> logger)
  {
    _settings = settings;
    _catalogManager = catalogManager;
    _tiers = tiers.OrderBy(t => t.Order).ToList();
    _logger = logger;
  }

  // ---------------------------------------------------------------------------
  //  Public entry point
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Runs every registered compaction tier for every stream,
  /// in <see cref="ICompactionTier.Order"/> order.
  /// </summary>
  /// <returns>Total number of source files consumed across all tiers.</returns>
  public async Task<int> CompactAllAsync(CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) {
      _logger.LogWarning("Compaction pipeline requires CatalogManager");
      return 0;
    }

    var streams = _catalogManager.GetStreams();
    var totalCompacted = 0;

    foreach (var stream in streams) {
      if (cancellationToken.IsCancellationRequested) break;

      try {
        foreach (var tier in _tiers) {
          if (cancellationToken.IsCancellationRequested) break;
          var count = await CompactStreamTierAsync(stream, tier, cancellationToken);
          totalCompacted += count;
        }
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to compact stream {Stream}", stream);
      }
    }

    return totalCompacted;
  }

  // ---------------------------------------------------------------------------
  //  Per-tier orchestration
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Runs a single <see cref="ICompactionTier"/> against one stream.
  /// </summary>
  /// <returns>Number of source files consumed.</returns>
  internal async Task<int> CompactStreamTierAsync(
      string stream,
      ICompactionTier tier,
      CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) return 0;

    var eligible = _catalogManager.GetEligibleEntries(
        stream, tier.InputLevel, tier.InputCompactionTier);

    if (eligible.Count == 0) return 0;

    var groups = tier.GroupEntries(eligible).ToList();
    var consolidatedCount = 0;

    foreach (var group in groups) {
      if (cancellationToken.IsCancellationRequested) break;

      if (!tier.IsGroupClosed(group.Key)) continue;

      var groupEntries = group.ToList();
      if (groupEntries.Count < tier.MinGroupSize) continue;

      try {
        await ConsolidateGroupAsync(stream, tier, group.Key, groupEntries, cancellationToken);
        consolidatedCount += groupEntries.Count;
      } catch (Exception ex) {
        _logger.LogError(ex,
            "{Tier} compaction failed for {Stream}/{GroupKey}",
            tier.Name, stream, group.Key);
      }
    }

    return consolidatedCount;
  }

  // ---------------------------------------------------------------------------
  //  Shared merge logic
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Merges the given catalog entries into a single Parquet file.
  /// Atomic commit order: write temp file → rename → update catalog → delete originals.
  /// </summary>
  private async Task ConsolidateGroupAsync(
      string stream,
      ICompactionTier tier,
      string groupKey,
      IReadOnlyList<CatalogEntry> sourceEntries,
      CancellationToken cancellationToken)
  {
    var logEntries = new List<LogEntry>();
    var sourceFiles = sourceEntries.Select(e => e.FilePath).ToList();

    foreach (var file in sourceFiles) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file, cancellationToken)) {
        logEntries.Add(entry);
      }
    }

    if (logEntries.Count == 0) {
      _logger.LogDebug("{Tier}: no entries to consolidate for {Stream}/{GroupKey}",
          tier.Name, stream, groupKey);
      return;
    }

    var minTime = logEntries.Min(e => e.Timestamp);
    var maxTime = logEntries.Max(e => e.Timestamp);

    var outputDir = Path.Combine(_settings.L2Directory, stream);
    Directory.CreateDirectory(outputDir);
    var outputFileName = tier.GetOutputFileName(stream, groupKey);
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));
    var tmpOutputPath = outputPath + ".tmp";

    await ParquetWriter.WriteBatchAsync(
        logEntries, tmpOutputPath, _settings.MaxDynamicKeys, cancellationToken);
    File.Move(tmpOutputPath, outputPath, overwrite: true);

    var fileInfo = new FileInfo(outputPath);
    var newEntry = new CatalogEntry {
      StreamName = stream,
      MinTime = minTime,
      MaxTime = maxTime,
      FilePath = outputPath,
      Level = StorageLevel.L2,
      RowCount = logEntries.Count,
      FileSizeBytes = fileInfo.Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = tier.OutputCompactionTier
    };

    await _catalogManager!.ReplaceFilesAsync(sourceFiles, newEntry, cancellationToken);

    _logger.LogInformation(
        "{Tier} compaction: {Count} entries from {Files} files → {Output}",
        tier.Name, logEntries.Count, sourceFiles.Count, outputFileName);

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