using Lumina.Core.Concurrency;
using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Result returned by <see cref="CompactionPipeline.CompactAllAsync"/>.
/// Contains the total number of source files consumed and a list of
/// source files whose deletion was deferred so the caller can delete
/// them under a writer lock.
/// </summary>
public sealed class CompactionResult
{
  /// <summary>Total number of source files consumed across all tiers.</summary>
  public int TotalCompacted { get; init; }

  /// <summary>
  /// Source files that were replaced in the catalog but not yet deleted
  /// from disk.  Grouped by stream name.
  /// </summary>
  public IReadOnlyDictionary<string, List<string>> PendingDeletions { get; init; }
      = new Dictionary<string, List<string>>();
}

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
  private readonly StreamLockManager? _streamLockManager;

  public CompactionPipeline(
      CompactionSettings settings,
      CatalogManager? catalogManager,
      IEnumerable<ICompactionTier> tiers,
      ILogger<CompactionPipeline> logger,
      StreamLockManager? streamLockManager = null)
  {
    _settings = settings;
    _catalogManager = catalogManager;
    _tiers = tiers.OrderBy(t => t.Order).ToList();
    _logger = logger;
    _streamLockManager = streamLockManager;
  }

  // ---------------------------------------------------------------------------
  //  Public entry point
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Runs every registered compaction tier for every stream,
  /// in <see cref="ICompactionTier.Order"/> order.
  /// Source file deletions are <b>deferred</b> and returned in
  /// <see cref="CompactionResult.PendingDeletions"/> so the caller can
  /// delete them under a writer lock.
  /// </summary>
  public async Task<CompactionResult> CompactAllAsync(CancellationToken cancellationToken = default)
  {
    var pendingDeletions = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

    if (_catalogManager == null) {
      _logger.LogWarning("Compaction pipeline requires CatalogManager");
      return new CompactionResult { TotalCompacted = 0, PendingDeletions = pendingDeletions };
    }

    var streams = _catalogManager.GetStreams();
    var totalCompacted = 0;

    foreach (var stream in streams) {
      if (cancellationToken.IsCancellationRequested) break;

      try {
        foreach (var tier in _tiers) {
          if (cancellationToken.IsCancellationRequested) break;
          var (count, filesToDelete) = await CompactStreamTierAsync(stream, tier, cancellationToken);
          totalCompacted += count;

          if (filesToDelete.Count > 0) {
            if (!pendingDeletions.TryGetValue(stream, out var list)) {
              list = new List<string>();
              pendingDeletions[stream] = list;
            }
            list.AddRange(filesToDelete);
          }
        }
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to compact stream {Stream}", stream);
      }
    }

    return new CompactionResult { TotalCompacted = totalCompacted, PendingDeletions = pendingDeletions };
  }

  // ---------------------------------------------------------------------------
  //  Per-tier orchestration
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Runs a single <see cref="ICompactionTier"/> against one stream.
  /// </summary>
  /// <returns>Number of source files consumed and list of files pending deletion.</returns>
  internal async Task<(int Count, List<string> PendingDeletions)> CompactStreamTierAsync(
      string stream,
      ICompactionTier tier,
      CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) return (0, []);

    var eligible = _catalogManager.GetEligibleEntries(
        stream, tier.InputLevel, tier.InputCompactionTier);

    if (eligible.Count == 0) return (0, []);

    var groups = tier.GroupEntries(eligible).ToList();
    var consolidatedCount = 0;
    var allPendingDeletions = new List<string>();

    foreach (var group in groups) {
      if (cancellationToken.IsCancellationRequested) break;

      if (!tier.IsGroupClosed(group.Key)) continue;

      var groupEntries = group.ToList();
      if (groupEntries.Count < tier.MinGroupSize) continue;

      try {
        var filesToDelete = await ConsolidateGroupAsync(stream, tier, group.Key, groupEntries, cancellationToken);
        consolidatedCount += groupEntries.Count;
        allPendingDeletions.AddRange(filesToDelete);
      } catch (Exception ex) {
        _logger.LogError(ex,
            "{Tier} compaction failed for {Stream}/{GroupKey}",
            tier.Name, stream, group.Key);
      }
    }

    return (consolidatedCount, allPendingDeletions);
  }

  // ---------------------------------------------------------------------------
  //  Shared merge logic
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Merges the given catalog entries into a single Parquet file.
  /// Atomic commit order: write temp file → rename → update catalog.
  /// Source file deletion is <b>deferred</b> — the returned list must be
  /// deleted by the caller under a writer lock.
  /// </summary>
  /// <returns>List of source file paths that should be deleted by the caller.</returns>
  private async Task<IReadOnlyList<string>> ConsolidateGroupAsync(
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
      return Array.Empty<string>();
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

    // Deletion is deferred — return the list for the caller to handle.
    return sourceFiles;
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Best-effort deletion of source files after a successful catalog commit.
  /// Public so that <see cref="CompactorService"/> can invoke it under a writer lock.
  /// </summary>
  public void DeleteSourceFiles(IReadOnlyList<string> files)
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