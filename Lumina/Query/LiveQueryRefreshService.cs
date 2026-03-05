using Lumina.Core.Configuration;
using Lumina.Storage.Wal;

namespace Lumina.Query;

/// <summary>
/// Background service that periodically materializes the WAL hot buffer
/// into DuckDB tables so recently-ingested entries are queryable
/// within approximately 1 second.
/// </summary>
public sealed class LiveQueryRefreshService : BackgroundService
{
  private readonly WalHotBuffer _hotBuffer;
  private readonly DuckDbQueryService _queryService;
  private readonly WalStartupReplayService _replayService;
  private readonly QuerySettings _settings;
  private readonly ILogger<LiveQueryRefreshService> _logger;
  private readonly Dictionary<string, long> _lastSeenVersions = new(StringComparer.OrdinalIgnoreCase);

  public LiveQueryRefreshService(
      WalHotBuffer hotBuffer,
      DuckDbQueryService queryService,
      WalStartupReplayService replayService,
      QuerySettings settings,
      ILogger<LiveQueryRefreshService> logger)
  {
    _hotBuffer = hotBuffer;
    _queryService = queryService;
    _replayService = replayService;
    _settings = settings;
    _logger = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    _logger.LogInformation("Live query refresh service starting (interval: {Interval}s)",
        _settings.LiveRefreshIntervalSeconds);

    // Wait for startup replay to finish so the buffer is pre-populated
    try {
      await _replayService.ReplayComplete.Task.WaitAsync(TimeSpan.FromSeconds(60), stoppingToken);
    } catch (TimeoutException) {
      _logger.LogWarning("WAL startup replay did not complete in 60s — starting live refresh anyway");
    }

    // Small initial delay to let DuckDB fully initialize
    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);

    var interval = TimeSpan.FromSeconds(_settings.LiveRefreshIntervalSeconds);

    while (!stoppingToken.IsCancellationRequested) {
      try {
        await RefreshChangedStreamsAsync(stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        break;
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Error during live query refresh");
      }

      try {
        await Task.Delay(interval, stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        break;
      }
    }

    _logger.LogInformation("Live query refresh service stopped");
  }

  private async Task RefreshChangedStreamsAsync(CancellationToken cancellationToken)
  {
    var streams = _hotBuffer.GetBufferedStreams();

    foreach (var stream in streams) {
      // Atomically capture both version and snapshot to eliminate the TOCTOU gap
      // where entries could be appended between GetStreamVersion and TakeSnapshot.
      var (currentVersion, snapshot) = _hotBuffer.TakeSnapshotWithVersion(stream);

      _lastSeenVersions.TryGetValue(stream, out var lastVersion);
      if (currentVersion == lastVersion) continue;

      try {
        await _queryService.RefreshHotBufferAsync(stream, snapshot, cancellationToken);
        _lastSeenVersions[stream] = currentVersion;
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to refresh hot buffer for stream '{Stream}'", stream);
      }
    }
  }
}
