using Lumina.Core.Configuration;

namespace Lumina.Storage.Wal;

/// <summary>
/// Background service that periodically flushes (fsyncs) all active WAL writers
/// to physical storage.  This provides bounded data-loss guarantees for the
/// non-WriteThrough path: at most <see cref="WalSettings.FlushIntervalMs"/>
/// worth of entries can be lost on an unclean shutdown.
///
/// When <see cref="WalSettings.EnableWriteThrough"/> is true, every write
/// already goes directly to disk; the periodic flush is a harmless no-op.
/// </summary>
public sealed class WalFlushService : BackgroundService
{
  private readonly WalManager _walManager;
  private readonly WalSettings _settings;
  private readonly ILogger<WalFlushService> _logger;

  public WalFlushService(
      WalManager walManager,
      WalSettings settings,
      ILogger<WalFlushService> logger)
  {
    _walManager = walManager;
    _settings = settings;
    _logger = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    // Nothing to do when the OS handles durability for us.
    if (_settings.EnableWriteThrough) {
      _logger.LogDebug("WAL flush service disabled: WriteThrough is enabled");
      return;
    }

    var interval = TimeSpan.FromMilliseconds(
        Math.Max(_settings.FlushIntervalMs, 50)); // floor at 50 ms

    _logger.LogInformation(
        "WAL flush service starting (interval: {Interval}ms)",
        interval.TotalMilliseconds);

    while (!stoppingToken.IsCancellationRequested) {
      try {
        await Task.Delay(interval, stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        break;
      }

      try {
        await _walManager.FlushAllWritersAsync(stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        break;
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Error during periodic WAL flush");
      }
    }

    // Final flush on shutdown to ensure all pending data reaches disk.
    try {
      await _walManager.FlushAllWritersAsync(CancellationToken.None);
      _logger.LogInformation("WAL flush service: final flush complete");
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Final WAL flush on shutdown failed");
    }
  }
}
