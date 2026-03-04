using Lumina.Storage.Compaction;

namespace Lumina.Storage.Wal;

/// <summary>
/// Replays uncompacted WAL entries into the <see cref="WalHotBuffer"/> at startup
/// so that entries written before a restart are immediately queryable.
/// </summary>
public sealed class WalStartupReplayService : IHostedService
{
  private readonly WalHotBuffer _hotBuffer;
  private readonly WalManager _walManager;
  private readonly CursorManager _cursorManager;
  private readonly ILogger<WalStartupReplayService> _logger;

  /// <summary>
  /// Signals when the startup replay is complete.
  /// Other services (e.g. LiveQueryRefreshService) can await this
  /// to avoid serving stale results during warm-up.
  /// </summary>
  public TaskCompletionSource ReplayComplete { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

  public WalStartupReplayService(
      WalHotBuffer hotBuffer,
      WalManager walManager,
      CursorManager cursorManager,
      ILogger<WalStartupReplayService> logger)
  {
    _hotBuffer = hotBuffer;
    _walManager = walManager;
    _cursorManager = cursorManager;
    _logger = logger;
  }

  public async Task StartAsync(CancellationToken cancellationToken)
  {
    try {
      var allWalFiles = _walManager.GetAllWalFiles();
      var totalReplayed = 0;

      foreach (var (stream, walFiles) in allWalFiles) {
        var cursor = _cursorManager.GetCursor(stream);
        var entries = new List<BufferedEntry>();

        foreach (var walFile in walFiles) {
          // Skip files that are fully compacted
          if (cursor.LastCompactedWalFile != null) {
            int cmp = string.Compare(walFile, cursor.LastCompactedWalFile, StringComparison.Ordinal);
            if (cmp < 0) continue;
          }

          try {
            using var reader = await _walManager.GetReaderAsync(walFile, stream, cancellationToken);

            await foreach (var walEntry in reader.ReadEntriesAsync(cancellationToken)) {
              // Skip entries already represented in Parquet
              if (cursor.LastCompactedWalFile != null &&
                  walFile == cursor.LastCompactedWalFile &&
                  walEntry.Offset <= cursor.LastCompactedOffset) {
                continue;
              }

              entries.Add(new BufferedEntry {
                WalFile = walFile,
                Offset = walEntry.Offset,
                LogEntry = walEntry.LogEntry
              });
            }
          } catch (Exception ex) {
            _logger.LogWarning(ex, "Failed to replay WAL file {File} for stream {Stream}", walFile, stream);
          }
        }

        if (entries.Count > 0) {
          _hotBuffer.AppendBatch(stream, entries);
          totalReplayed += entries.Count;
          _logger.LogDebug("Replayed {Count} uncompacted entries for stream {Stream}", entries.Count, stream);
        }
      }

      _logger.LogInformation("WAL startup replay complete: {Count} entries loaded into hot buffer", totalReplayed);
      ReplayComplete.TrySetResult();
    } catch (Exception ex) {
      _logger.LogError(ex, "WAL startup replay failed");
      ReplayComplete.TrySetException(ex);
    }
  }

  public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
