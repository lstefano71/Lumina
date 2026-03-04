using Lumina.Core.Models;
using Lumina.Ingestion.Models;
using Lumina.Storage.Wal;

using Microsoft.AspNetCore.Mvc;

namespace Lumina.Ingestion.Endpoints;

/// <summary>
/// OTLP (OpenTelemetry Protocol) ingestion endpoint.
/// Supports HTTP/JSON formats for log ingestion.
/// </summary>
public static class OtlpIngestionEndpoint
{
  public static void MapOtlpEndpoints(this IEndpointRouteBuilder app)
  {
    var group = app.MapGroup("/v1/otlp");

    // OTLP HTTP endpoint for logs
    group.MapPost("/v1/logs", IngestOtlpLogsAsync)
        .WithName("IngestOTLPLogs")
        .WithDescription("Ingest logs via OpenTelemetry Protocol (HTTP/JSON)")
        .Accepts<OtlpLogsRequest>("application/json")
        .Produces<IngestResponse>(StatusCodes.Status200OK)
        .Produces(StatusCodes.Status400BadRequest);

    // OTLP HTTP endpoint for metrics (placeholder)
    group.MapPost("/v1/metrics", IngestOtlpMetricsAsync)
        .WithName("IngestOTLPMetrics")
        .WithDescription("Ingest metrics via OpenTelemetry Protocol (placeholder)")
        .Produces(StatusCodes.Status501NotImplemented);

    // OTLP HTTP endpoint for traces (placeholder)
    group.MapPost("/v1/traces", IngestOtlpTracesAsync)
        .WithName("IngestOTLPTraces")
        .WithDescription("Ingest traces via OpenTelemetry Protocol (placeholder)")
        .Produces(StatusCodes.Status501NotImplemented);
  }

  private static async Task<IResult> IngestOtlpLogsAsync(
      [FromBody] OtlpLogsRequest request,
      [FromServices] WalManager walManager,
      [FromServices] WalHotBuffer hotBuffer,
      [FromServices] ILoggerFactory loggerFactory,
      CancellationToken cancellationToken)
  {
    if (request.ResourceLogs == null || request.ResourceLogs.Count == 0) {
      return Results.BadRequest("No resource logs provided");
    }

    var totalIngested = 0;
    var errors = new List<string>();

    foreach (var resourceLog in request.ResourceLogs) {
      var resourceAttrs = resourceLog.Resource?.Attributes ?? new Dictionary<string, OtlpAnyValue>();
      var serviceName = "unknown";

      if (resourceAttrs.TryGetValue("service.name", out var serviceNameValue) && serviceNameValue?.StringValue != null) {
        serviceName = serviceNameValue.StringValue;
      }

      var scopeLogs = resourceLog.ScopeLogs ?? new List<OtlpScopeLogs>();

      foreach (var scopeLog in scopeLogs) {
        var logRecords = scopeLog.LogRecords ?? new List<OtlpLogRecord>();

        foreach (var logRecord in logRecords) {
          try {
            var logEntry = ConvertOtlpLogToEntry(logRecord, serviceName, scopeLog.Scope);

            var writer = await walManager.GetOrCreateWriterAsync(logEntry.Stream, cancellationToken);
            var offset = await writer.WriteAsync(logEntry, cancellationToken);

            // Push to hot buffer for sub-second query visibility
            hotBuffer.Append(logEntry.Stream, writer.FilePath, offset, logEntry);

            totalIngested++;
          } catch (Exception ex) {
            errors.Add($"Failed to ingest log: {ex.Message}");
            var logger = loggerFactory.CreateLogger("OtlpIngestion");
            logger.LogWarning(ex, "Failed to ingest OTLP log record");
          }
        }
      }
    }

    return Results.Ok(new IngestResponse {
      Success = errors.Count == 0,
      EntriesAccepted = totalIngested,
      Timestamp = DateTime.UtcNow,
      Error = errors.Count > 0 ? string.Join("; ", errors) : null
    });
  }

  private static Task<IResult> IngestOtlpMetricsAsync()
  {
    return Task.FromResult(Results.StatusCode(StatusCodes.Status501NotImplemented));
  }

  private static Task<IResult> IngestOtlpTracesAsync()
  {
    return Task.FromResult(Results.StatusCode(StatusCodes.Status501NotImplemented));
  }

  private static LogEntry ConvertOtlpLogToEntry(OtlpLogRecord logRecord, string serviceName, OtlpInstrumentationScope? scope)
  {
    var attributes = new Dictionary<string, object?>();

    // Add OTLP attributes
    if (logRecord.Attributes != null) {
      foreach (var attr in logRecord.Attributes) {
        attributes[attr.Key] = attr.Value?.ToObject();
      }
    }

    // Add scope information
    if (scope != null) {
      attributes["scope.name"] = scope.Name;
      attributes["scope.version"] = scope.Version;
    }

    // Add trace context
    if (logRecord.TraceId != null) {
      attributes["trace_id"] = logRecord.TraceId;
    }

    if (logRecord.SpanId != null) {
      attributes["span_id"] = logRecord.SpanId;
    }

    // Convert severity to level
    var level = logRecord.SeverityNumber switch {
      <= 5 => "debug",     // TRACE, DEBUG
      <= 9 => "info",      // INFO, INFO2-4
      <= 13 => "warn",     // WARN, WARN2-4
      <= 17 => "error",    // ERROR, ERROR2-4
      _ => "fatal"          // FATAL
    };

    // Convert timestamp (nanoseconds since epoch to milliseconds)
    var timestamp = logRecord.TimeUnixNano > 0
        ? DateTimeOffset.FromUnixTimeMilliseconds(logRecord.TimeUnixNano / 1_000_000).UtcDateTime
        : DateTime.UtcNow;

    return new LogEntry {
      Stream = serviceName,
      Timestamp = timestamp,
      Level = level,
      Message = logRecord.Body?.StringValue ?? "",
      Attributes = attributes,
      TraceId = logRecord.TraceId,
      SpanId = logRecord.SpanId
    };
  }
}

#region OTLP Models

/// <summary>
/// OTLP logs request.
/// </summary>
public sealed class OtlpLogsRequest
{
  public List<OtlpResourceLogs>? ResourceLogs { get; init; }
}

public sealed class OtlpResourceLogs
{
  public OtlpResource? Resource { get; init; }
  public List<OtlpScopeLogs>? ScopeLogs { get; init; }
}

public sealed class OtlpResource
{
  public Dictionary<string, OtlpAnyValue>? Attributes { get; init; }
}

public sealed class OtlpScopeLogs
{
  public OtlpInstrumentationScope? Scope { get; init; }
  public List<OtlpLogRecord>? LogRecords { get; init; }
}

public sealed class OtlpInstrumentationScope
{
  public string? Name { get; init; }
  public string? Version { get; init; }
}

public sealed class OtlpLogRecord
{
  public long TimeUnixNano { get; init; }
  public int SeverityNumber { get; init; }
  public string? SeverityText { get; init; }
  public OtlpAnyValue? Body { get; init; }
  public Dictionary<string, OtlpAnyValue>? Attributes { get; init; }
  public string? TraceId { get; init; }
  public string? SpanId { get; init; }
}

public sealed class OtlpAnyValue
{
  public string? StringValue { get; init; }
  public long IntValue { get; init; }
  public double DoubleValue { get; init; }
  public bool BoolValue { get; init; }
  public List<OtlpAnyValue>? ArrayValue { get; init; }
  public Dictionary<string, OtlpAnyValue>? KvListValue { get; init; }

  public object? ToObject()
  {
    if (StringValue != null) return StringValue;
    if (IntValue != 0) return IntValue;
    if (DoubleValue != 0) return DoubleValue;
    if (BoolValue) return BoolValue;
    if (ArrayValue != null) return ArrayValue.Select(v => v.ToObject()).ToList();
    if (KvListValue != null) return KvListValue.ToDictionary(k => k.Key, k => k.Value.ToObject());
    return null;
  }
}

#endregion