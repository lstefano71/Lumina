using Lumina.Core.Concurrency;
using Lumina.Core.Configuration;
using Lumina.Query;
using Lumina.Storage.Wal;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Background service that runs periodic WAL → Parquet compaction (L1)
/// and calendar-based consolidation (L2).
/// Triggers a DuckDB view refresh whenever files change so queries
/// always see the latest catalog state.
/// </summary>
public sealed class CompactorService : BackgroundService
{
  private readonly L1Compactor _l1Compactor;
  private readonly CompactionPipeline _compactionPipeline;
  private readonly DuckDbQueryService _queryService;
  private readonly WalHotBuffer _hotBuffer;
  private readonly StreamLockManager _streamLockManager;
  private readonly CompactionSettings _settings;
  private readonly ILogger<CompactorService> _logger;
  private readonly IHostApplicationLifetime _lifetime;
  private DateTime _lastL2Run = DateTime.MinValue;

  public CompactorService(
      L1Compactor l1Compactor,
      CompactionPipeline compactionPipeline,
      DuckDbQueryService queryService,
        WalHotBuffer hotBuffer,
      StreamLockManager streamLockManager,
      CompactionSettings settings,
      ILogger<CompactorService> logger,
      IHostApplicationLifetime lifetime)
  {
    _l1Compactor = l1Compactor;
    _compactionPipeline = compactionPipeline;
    _queryService = queryService;
    _hotBuffer = hotBuffer;
    _streamLockManager = streamLockManager;
    _settings = settings;
    _logger = logger;
    _lifetime = lifetime;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    _logger.LogInformation("Compactor service starting...");

    CleanupOrphanedTempFiles();

    // Wait for the application to be fully started
    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

    var interval = TimeSpan.FromMinutes(_settings.IntervalMinutes);

    while (!stoppingToken.IsCancellationRequested) {
      try {
        await RunCompactionAsync(stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        // Expected during shutdown
        break;
      } catch (Exception ex) {
        _logger.LogError(ex, "Error during compaction run");
      }

      try {
        await Task.Delay(interval, stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        break;
      }
    }

    _logger.LogInformation("Compactor service stopped");
  }

  private async Task RunCompactionAsync(CancellationToken cancellationToken)
  {
    _logger.LogDebug("Starting compaction run");
    var filesChanged = false;

    // Hold writer lock for the full compaction cycle so queries cannot observe
    // transient overlap states while L1 compaction writes parquet and hot data
    // is being evicted/reconciled.
    await using var writerGuard = await _streamLockManager.CompactionLock
        .WriterLockAsync(cancellationToken).ConfigureAwait(false);

    // Run L1 compaction (WAL → Parquet)
    var entriesCompacted = await _l1Compactor.CompactAllAsync(cancellationToken);

    if (entriesCompacted > 0) {
      filesChanged = true;
      _logger.LogInformation("L1 compaction complete: {Count} entries compacted", entriesCompacted);
    } else {
      _logger.LogDebug("L1 compaction complete: no entries to compact");
    }

    // Run compaction pipeline (daily → monthly → …) if interval has elapsed
    var l2Interval = TimeSpan.FromHours(_settings.L2IntervalHours);
    var timeSinceLastL2 = DateTime.UtcNow - _lastL2Run;

    CompactionResult? compactionResult = null;
    if (timeSinceLastL2 >= l2Interval) {
      _logger.LogInformation("Starting compaction pipeline");
      compactionResult = await _compactionPipeline.CompactAllAsync(cancellationToken);
      _lastL2Run = DateTime.UtcNow;

      if (compactionResult.TotalCompacted > 0) {
        filesChanged = true;
        _logger.LogInformation("Compaction pipeline complete: {Count} files consolidated", compactionResult.TotalCompacted);
      } else {
        _logger.LogDebug("Compaction pipeline complete: no files to consolidate");
      }
    } else {
      _logger.LogDebug(
          "Skipping compaction pipeline (next run in {Remaining})",
          l2Interval - timeSinceLastL2);
    }

    // Refresh DuckDB views and delete old source files while writer lock is held.
    if (filesChanged) {
      try {
        await _queryService.RefreshStreamsAsync(cancellationToken);
        await ReconcileHotTablesAfterCompactionAsync(cancellationToken);
        _logger.LogDebug("DuckDB views refreshed after compaction");
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to refresh DuckDB views after compaction");
      }

      // Now that views point to the new merged files, safely delete the old ones.
      if (compactionResult?.PendingDeletions is { Count: > 0 } pending) {
        foreach (var (stream, files) in pending) {
          _compactionPipeline.DeleteSourceFiles(files);
          _logger.LogDebug("Deleted {Count} old source files for stream {Stream}", files.Count, stream);
        }
      }
    }
  }

  private async Task ReconcileHotTablesAfterCompactionAsync(CancellationToken cancellationToken)
  {
    var hotStreams = _hotBuffer.GetBufferedStreams().ToHashSet(StringComparer.OrdinalIgnoreCase);
    var registeredStreams = _queryService.GetRegisteredStreams();

    // First, bring all hot-backed streams to an exact post-eviction snapshot so
    // compaction does not temporarily double-count rows (new parquet + stale hot table).
    foreach (var stream in hotStreams) {
      cancellationToken.ThrowIfCancellationRequested();

      var snapshot = _hotBuffer.TakeSnapshot(stream);
      await _queryService.RefreshHotBufferAsync(stream, snapshot, cancellationToken);
    }

    foreach (var stream in registeredStreams) {
      cancellationToken.ThrowIfCancellationRequested();

      if (hotStreams.Contains(stream)) {
        continue;
      }

      await _queryService.ClearHotTableAsync(stream, cancellationToken);
      await _queryService.RebuildStreamViewAsync(stream, cancellationToken);
    }
  }

  private void CleanupOrphanedTempFiles()
  {
    if (!Directory.Exists(_settings.L2Directory)) return;

    try {
      var tmpFiles = Directory.EnumerateFiles(
          _settings.L2Directory, "*.tmp", SearchOption.AllDirectories);

      foreach (var tmp in tmpFiles) {
        try {
          File.Delete(tmp);
          _logger.LogWarning("Deleted orphaned temp file from previous crash: {File}", tmp);
        } catch (Exception ex) {
          _logger.LogWarning(ex, "Failed to delete orphaned temp file: {File}", tmp);
        }
      }
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Error scanning for orphaned temp files in {Dir}", _settings.L2Directory);
    }
  }
}