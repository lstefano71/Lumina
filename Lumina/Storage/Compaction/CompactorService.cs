using Lumina.Core.Configuration;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Background service that runs periodic WAL → Parquet compaction.
/// </summary>
public sealed class CompactorService : BackgroundService
{
    private readonly L1Compactor _l1Compactor;
    private readonly CompactionSettings _settings;
    private readonly ILogger<CompactorService> _logger;
    private readonly IHostApplicationLifetime _lifetime;
    
    public CompactorService(
        L1Compactor l1Compactor,
        CompactionSettings settings,
        ILogger<CompactorService> logger,
        IHostApplicationLifetime lifetime)
    {
        _l1Compactor = l1Compactor;
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
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunCompactionAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during compaction run");
            }
            
            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
        
        _logger.LogInformation("Compactor service stopped");
    }
    
    private async Task RunCompactionAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting compaction run");
        
        var entriesCompacted = await _l1Compactor.CompactAllAsync(cancellationToken);
        
        if (entriesCompacted > 0)
        {
            _logger.LogInformation("Compaction complete: {Count} entries compacted", entriesCompacted);
        }
        else
        {
            _logger.LogDebug("Compaction complete: no entries to compact");
        }
    }
}