using Lumina.Core.Configuration;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Background service that runs periodic WAL → Parquet compaction (L1)
/// and daily consolidation (L2).
/// </summary>
public sealed class CompactorService : BackgroundService
{
  private readonly L1Compactor _l1Compactor;
  private readonly L2Compactor _l2Compactor;
  private readonly CompactionSettings _settings;
  private readonly ILogger<CompactorService> _logger;
  private readonly IHostApplicationLifetime _lifetime;
  private DateTime _lastL2Run = DateTime.MinValue;

  public CompactorService(
      L1Compactor l1Compactor,
      L2Compactor l2Compactor,
      CompactionSettings settings,
      ILogger<CompactorService> logger,
      IHostApplicationLifetime lifetime)
  {
    _l1Compactor = l1Compactor;
    _l2Compactor = l2Compactor;
    _settings = settings;
    _logger = logger;
    _lifetime = lifetime;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    _logger.LogInformation("Compactor service starting...");

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

    // Run L1 compaction (WAL → Parquet)
    var entriesCompacted = await _l1Compactor.CompactAllAsync(cancellationToken);

    if (entriesCompacted > 0) {
      _logger.LogInformation("L1 compaction complete: {Count} entries compacted", entriesCompacted);
    } else {
      _logger.LogDebug("L1 compaction complete: no entries to compact");
    }

    // Run L2 compaction (daily consolidation) if interval has elapsed
    var l2Interval = TimeSpan.FromHours(_settings.L2IntervalHours);
    var timeSinceLastL2 = DateTime.UtcNow - _lastL2Run;

    if (timeSinceLastL2 >= l2Interval) {
      _logger.LogInformation("Starting L2 compaction (daily consolidation)");
      var filesConsolidated = await _l2Compactor.CompactAllAsync(cancellationToken);
      _lastL2Run = DateTime.UtcNow;

      if (filesConsolidated > 0) {
        _logger.LogInformation("L2 compaction complete: {Count} files consolidated", filesConsolidated);
      } else {
        _logger.LogDebug("L2 compaction complete: no files to consolidate");
      }
    } else {
      _logger.LogDebug(
          "Skipping L2 compaction (next run in {Remaining})",
          l2Interval - timeSinceLastL2);
    }
  }
}