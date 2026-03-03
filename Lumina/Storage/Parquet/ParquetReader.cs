using Apache.Arrow;
using Apache.Arrow.Ipc;

using Lumina.Core.Models;

using System.Text.Json;

namespace Lumina.Storage.Parquet;

/// <summary>
/// Reads log entries from Parquet/Arrow files.
/// </summary>
public static class ParquetReader
{
  /// <summary>
  /// Reads all log entries from an Arrow IPC file.
  /// </summary>
  /// <param name="filePath">The file path to read.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>An async enumerable of log entries.</returns>
  public static async IAsyncEnumerable<LogEntry> ReadEntriesAsync(
      string filePath,
      [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
    using var reader = new ArrowStreamReader(fileStream);

    // Schema is read automatically with the first record batch

    while (true) {
      var recordBatch = await reader.ReadNextRecordBatchAsync(cancellationToken);
      if (recordBatch == null || recordBatch.Length == 0) {
        break;
      }

      foreach (var entry in ParseRecordBatch(recordBatch)) {
        yield return entry;
      }
    }
  }

  /// <summary>
  /// Reads a batch of log entries from an Arrow file.
  /// </summary>
  /// <param name="filePath">The file path to read.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The list of log entries.</returns>
  public static async Task<IReadOnlyList<LogEntry>> ReadBatchAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    var entries = new List<LogEntry>();

    await foreach (var entry in ReadEntriesAsync(filePath, cancellationToken)) {
      entries.Add(entry);
    }

    return entries;
  }

  private static IEnumerable<LogEntry> ParseRecordBatch(RecordBatch batch)
  {
    var columnNames = batch.Schema.FieldsList.Select(f => f.Name).ToList();
    var rowCount = batch.Length;

    // Get column indices
    var streamCol = GetColumnIndex(batch, "stream");
    var timestampCol = GetColumnIndex(batch, "timestamp");
    var levelCol = GetColumnIndex(batch, "level");
    var messageCol = GetColumnIndex(batch, "message");
    var traceIdCol = GetColumnIndex(batch, "trace_id");
    var spanIdCol = GetColumnIndex(batch, "span_id");
    var durationMsCol = GetColumnIndex(batch, "duration_ms");
    var metaCol = GetColumnIndex(batch, "_meta");

    for (int i = 0; i < rowCount; i++) {
      var entry = new LogEntry {
        Stream = streamCol >= 0 ? GetStringColumnValue(batch, streamCol, i) : "unknown",
        Timestamp = timestampCol >= 0 ? GetTimestampColumnValue(batch, timestampCol, i) : DateTime.UtcNow,
        Level = levelCol >= 0 ? GetStringColumnValue(batch, levelCol, i) : "info",
        Message = messageCol >= 0 ? GetStringColumnValue(batch, messageCol, i) : "",
        TraceId = traceIdCol >= 0 ? GetNullableStringColumnValue(batch, traceIdCol, i) : null,
        SpanId = spanIdCol >= 0 ? GetNullableStringColumnValue(batch, spanIdCol, i) : null,
        DurationMs = durationMsCol >= 0 ? GetNullableIntColumnValue(batch, durationMsCol, i) : null,
        Attributes = new Dictionary<string, object?>()
      };

      // Parse dynamic columns (not fixed columns)
      var fixedColumns = new HashSet<string> { "stream", "timestamp", "level", "message", "trace_id", "span_id", "duration_ms", "_meta" };

      for (int colIdx = 0; colIdx < batch.ColumnCount; colIdx++) {
        var colName = columnNames[colIdx];
        if (fixedColumns.Contains(colName)) {
          continue;
        }

        var value = GetColumnValue(batch, colIdx, i);
        if (value != null) {
          entry.Attributes[colName] = value;
        }
      }

      // Parse _meta overflow column
      if (metaCol >= 0) {
        var metaJson = GetNullableStringColumnValue(batch, metaCol, i);
        if (!string.IsNullOrEmpty(metaJson)) {
          try {
            var meta = JsonSerializer.Deserialize<Dictionary<string, object?>>(metaJson);
            if (meta != null) {
              foreach (var kvp in meta) {
                entry.Attributes[kvp.Key] = kvp.Value;
              }
            }
          } catch {
            // Ignore JSON parse errors
          }
        }
      }

      yield return entry;
    }
  }

  private static int GetColumnIndex(RecordBatch batch, string columnName)
  {
    for (int i = 0; i < batch.Schema.FieldsList.Count; i++) {
      if (batch.Schema.FieldsList[i].Name == columnName) {
        return i;
      }
    }
    return -1;
  }

  private static string GetStringColumnValue(RecordBatch batch, int columnIndex, int rowIndex)
  {
    var column = batch.Column(columnIndex);
    if (column is StringArray stringArray) {
      return stringArray.GetString(rowIndex);
    }
    return "";
  }

  private static string? GetNullableStringColumnValue(RecordBatch batch, int columnIndex, int rowIndex)
  {
    var column = batch.Column(columnIndex);
    if (column is StringArray stringArray) {
      if (stringArray.IsNull(rowIndex)) {
        return null;
      }
      return stringArray.GetString(rowIndex);
    }
    return null;
  }

  private static int? GetNullableIntColumnValue(RecordBatch batch, int columnIndex, int rowIndex)
  {
    var column = batch.Column(columnIndex);
    if (column is Int32Array intArray) {
      if (intArray.IsNull(rowIndex)) {
        return null;
      }
      return intArray.GetValue(rowIndex);
    }
    return null;
  }

  private static DateTime GetTimestampColumnValue(RecordBatch batch, int columnIndex, int rowIndex)
  {
    var column = batch.Column(columnIndex);
    if (column is TimestampArray timestampArray) {
      var dto = timestampArray.GetTimestamp(rowIndex);
      if (dto.HasValue) {
        return dto.Value.UtcDateTime;
      }
    }
    return DateTime.UtcNow;
  }

  private static object? GetColumnValue(RecordBatch batch, int columnIndex, int rowIndex)
  {
    var column = batch.Column(columnIndex);

    if (column.IsNull(rowIndex)) {
      return null;
    }

    if (column is StringArray str) {
      return str.GetString(rowIndex);
    }
    if (column is Int32Array i32) {
      return i32.GetValue(rowIndex);
    }
    if (column is Int64Array i64) {
      return i64.GetValue(rowIndex);
    }
    if (column is FloatArray f32) {
      return f32.GetValue(rowIndex);
    }
    if (column is DoubleArray f64) {
      return f64.GetValue(rowIndex);
    }
    if (column is BooleanArray b) {
      return b.GetValue(rowIndex);
    }
    if (column is TimestampArray ts) {
      var dto = ts.GetTimestamp(rowIndex);
      if (dto.HasValue) {
        return dto.Value.UtcDateTime;
      }
      return null;
    }
    if (column is BinaryArray bin) {
      return bin.GetBytes(rowIndex).ToArray();
    }

    return null;
  }
}