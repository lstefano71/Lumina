using Lumina.Core.Configuration;

namespace Lumina.Query;

/// <summary>
/// Background service that periodically discovers and registers streams as DuckDB views.
/// </summary>
public sealed class StreamDiscoveryService : BackgroundService
{
  private readonly DuckDbQueryService _queryService;
  private readonly QuerySettings _settings;
  private readonly ILogger<StreamDiscoveryService> _logger;

  public StreamDiscoveryService(
      DuckDbQueryService queryService,
      QuerySettings settings,
      ILogger<StreamDiscoveryService> logger)
  {
    _queryService = queryService;
    _settings = settings;
    _logger = logger;
  }

  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    _logger.LogInformation("Stream discovery service started with refresh interval: {Interval}s",
        _settings.RefreshStreamsIntervalSeconds);

    // Initial delay to allow the application to start up
    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

    while (!stoppingToken.IsCancellationRequested) {
      try {
        await RefreshStreamsAsync(stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        // Expected during shutdown
        break;
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Error during stream discovery refresh");
      }

      try {
        await Task.Delay(_settings.RefreshInterval, stoppingToken);
      } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
        // Expected during shutdown
        break;
      }
    }

    _logger.LogInformation("Stream discovery service stopped");
  }

  /// <summary>
  /// Performs a single stream discovery and registration refresh.
  /// </summary>
  private async Task RefreshStreamsAsync(CancellationToken cancellationToken)
  {
    _logger.LogDebug("Performing scheduled stream discovery refresh");
    await _queryService.RefreshStreamsAsync(cancellationToken);
  }
}