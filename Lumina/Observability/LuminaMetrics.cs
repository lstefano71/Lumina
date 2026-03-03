using System.Diagnostics.Metrics;

namespace Lumina.Observability;

/// <summary>
/// Centralized metrics for the Lumina engine.
/// Exposes counters and histograms for WAL corruption, ingestion throughput,
/// and compaction lag via System.Diagnostics.Metrics (compatible with
/// prometheus-net OpenTelemetry exporters and the built-in /metrics endpoint).
/// </summary>
public sealed class LuminaMetrics
{
  public const string MeterName = "Lumina";

  private readonly Meter _meter;

  private readonly Counter<long> _walCorruptionsDetected;
  private readonly Counter<long> _ingestionRateBytes;
  private readonly Histogram<double> _compactionLagMs;
  private readonly Counter<long> _entriesIngested;
  private readonly Counter<long> _entriesCompacted;

  public LuminaMetrics(IMeterFactory meterFactory)
  {
    _meter = meterFactory.Create(MeterName);

    _walCorruptionsDetected = _meter.CreateCounter<long>(
        "lumina.wal_corruptions_detected",
        unit: "{corruption}",
        description: "Total number of WAL corruption events detected during reads.");

    _ingestionRateBytes = _meter.CreateCounter<long>(
        "lumina.ingestion_rate_bytes",
        unit: "By",
        description: "Total bytes ingested through the WAL write path.");

    _compactionLagMs = _meter.CreateHistogram<double>(
        "lumina.compaction_lag_ms",
        unit: "ms",
        description: "Time in milliseconds for a compaction cycle to complete.");

    _entriesIngested = _meter.CreateCounter<long>(
        "lumina.entries_ingested",
        unit: "{entry}",
        description: "Total number of log entries ingested.");

    _entriesCompacted = _meter.CreateCounter<long>(
        "lumina.entries_compacted",
        unit: "{entry}",
        description: "Total number of log entries compacted to Parquet.");
  }

  /// <summary>
  /// Records a WAL corruption detection event.
  /// </summary>
  public void RecordWalCorruption(string stream) =>
      _walCorruptionsDetected.Add(1, new KeyValuePair<string, object?>("stream", stream));

  /// <summary>
  /// Records bytes written through the ingestion path.
  /// </summary>
  public void RecordIngestionBytes(long bytes, string stream) =>
      _ingestionRateBytes.Add(bytes,
          new KeyValuePair<string, object?>("stream", stream));

  /// <summary>
  /// Records the duration of a compaction cycle in milliseconds.
  /// </summary>
  public void RecordCompactionLag(double milliseconds, string stream) =>
      _compactionLagMs.Record(milliseconds,
          new KeyValuePair<string, object?>("stream", stream));

  /// <summary>
  /// Records the number of entries ingested.
  /// </summary>
  public void RecordEntriesIngested(long count, string stream) =>
      _entriesIngested.Add(count,
          new KeyValuePair<string, object?>("stream", stream));

  /// <summary>
  /// Records the number of entries compacted to Parquet.
  /// </summary>
  public void RecordEntriesCompacted(long count, string stream) =>
      _entriesCompacted.Add(count,
          new KeyValuePair<string, object?>("stream", stream));
}
