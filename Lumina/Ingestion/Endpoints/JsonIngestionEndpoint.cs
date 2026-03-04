using Lumina.Ingestion.Models;
using Lumina.Ingestion.Normalization;
using Lumina.Storage.Wal;

namespace Lumina.Ingestion.Endpoints;

/// <summary>
/// HTTP JSON ingestion endpoint for log entries.
/// </summary>
public static class JsonIngestionEndpoint
{
  /// <summary>
  /// Maps the JSON ingestion endpoints.
  /// </summary>
  /// <param name="app">The web application.</param>
  public static void MapEndpoints(WebApplication app)
  {
    app.MapPost("/v1/logs", HandleSingle)
        .WithName("IngestLog")
        .WithDescription("Ingest a single log entry")
        .DisableAntiforgery();

    app.MapPost("/v1/logs/batch", HandleBatch)
        .WithName("IngestLogBatch")
        .WithDescription("Ingest a batch of log entries")
        .DisableAntiforgery();
  }

  /// <summary>
  /// Handles single log ingestion.
  /// </summary>
  public static async Task<IResult> HandleSingle(
      LogIngestRequest request,
      WalManager walManager,
      WalHotBuffer hotBuffer,
      CancellationToken cancellationToken)
  {
    try {
      // Validate request
      if (string.IsNullOrWhiteSpace(request.Stream)) {
        return Results.BadRequest(IngestResponse.Fail("Stream name is required."));
      }

      if (string.IsNullOrWhiteSpace(request.Message)) {
        return Results.BadRequest(IngestResponse.Fail("Message is required."));
      }

      // Normalize to LogEntry
      var entry = JsonNormalizer.Normalize(request);

      // Get writer and write
      var writer = await walManager.GetOrCreateWriterAsync(entry.Stream, cancellationToken);
      var offset = await writer.WriteAsync(entry, cancellationToken);

      // Push to hot buffer for sub-second query visibility
      hotBuffer.Append(entry.Stream, writer.FilePath, offset, entry);

      // Check for rotation
      await walManager.RotateWalIfNeededAsync(entry.Stream, cancellationToken);

      return Results.Ok(IngestResponse.Ok(1));
    } catch (ArgumentException ex) {
      return Results.BadRequest(IngestResponse.Fail(ex.Message));
    } catch (Exception ex) {
      return Results.Problem(
          detail: ex.Message,
          statusCode: 500,
          title: "Internal server error during log ingestion");
    }
  }

  /// <summary>
  /// Handles batch log ingestion.
  /// </summary>
  public static async Task<IResult> HandleBatch(
      BatchLogIngestRequest request,
      WalManager walManager,
      WalHotBuffer hotBuffer,
      CancellationToken cancellationToken)
  {
    try {
      // Validate request
      if (string.IsNullOrWhiteSpace(request.Stream)) {
        return Results.BadRequest(IngestResponse.Fail("Stream name is required."));
      }

      if (request.Entries == null || request.Entries.Count == 0) {
        return Results.BadRequest(IngestResponse.Fail("Entries array is required and cannot be empty."));
      }

      // Normalize to LogEntry objects
      var entries = JsonNormalizer.NormalizeBatch(request);

      // Group by stream for efficient writes
      var grouped = entries.GroupBy(e => e.Stream);
      var totalAccepted = 0;

      foreach (var group in grouped) {
        var stream = group.Key;
        var streamEntries = group.ToList();

        // Get writer and write batch
        var writer = await walManager.GetOrCreateWriterAsync(stream, cancellationToken);
        var offsets = await writer.WriteBatchAsync(streamEntries, cancellationToken);

        // Push to hot buffer for sub-second query visibility
        for (int i = 0; i < streamEntries.Count; i++) {
          hotBuffer.Append(stream, writer.FilePath, offsets[i], streamEntries[i]);
        }

        // Check for rotation
        await walManager.RotateWalIfNeededAsync(stream, cancellationToken);

        totalAccepted += streamEntries.Count;
      }

      return Results.Ok(IngestResponse.Ok(totalAccepted));
    } catch (ArgumentException ex) {
      return Results.BadRequest(IngestResponse.Fail(ex.Message));
    } catch (Exception ex) {
      return Results.Problem(
          detail: ex.Message,
          statusCode: 500,
          title: "Internal server error during batch log ingestion");
    }
  }
}