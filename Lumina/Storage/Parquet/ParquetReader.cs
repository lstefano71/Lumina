using Lumina.Core.Models;

using Parquet.Data;

using System.Text.Json;

namespace Lumina.Storage.Parquet;

/// <summary>
/// Reads log entries from Parquet files using parquet-dotnet.
/// </summary>
public static class ParquetReader
{
  private static readonly HashSet<string> FixedColumns =
      new() { "stream", "timestamp", "level", "message", "trace_id", "span_id", "duration_ms", "_meta" };

  /// <summary>
  /// Reads all log entries from a Parquet file.
  /// </summary>
  /// <param name="filePath">The file path to read.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>An async enumerable of log entries.</returns>
  public static async IAsyncEnumerable<LogEntry> ReadEntriesAsync(
      string filePath,
      [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
    using var reader = await global::Parquet.ParquetReader.CreateAsync(fileStream, cancellationToken: cancellationToken);

    var dataFields = reader.Schema.GetDataFields();

    for (int groupIdx = 0; groupIdx < reader.RowGroupCount; groupIdx++) {
      cancellationToken.ThrowIfCancellationRequested();

      using var rowGroupReader = reader.OpenRowGroupReader(groupIdx);

      // Read all columns for this row group
      var columns = new Dictionary<string, DataColumn>(dataFields.Length);
      foreach (var field in dataFields) {
        var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken);
        columns[field.Name] = column;
      }

      if (columns.Count == 0) {
        continue;
      }

      var rowCount = columns.Values.First().Data.Length;

      for (int i = 0; i < rowCount; i++) {
        cancellationToken.ThrowIfCancellationRequested();

        var entry = new LogEntry {
          Stream = GetString(columns, "stream", i) ?? "unknown",
          Timestamp = GetDateTime(columns, "timestamp", i) ?? DateTime.UtcNow,
          Level = GetString(columns, "level", i) ?? "info",
          Message = GetString(columns, "message", i) ?? "",
          TraceId = GetString(columns, "trace_id", i),
          SpanId = GetString(columns, "span_id", i),
          DurationMs = GetNullableInt(columns, "duration_ms", i),
          Attributes = new Dictionary<string, object?>()
        };

        // Parse dynamic attribute columns
        foreach (var (name, column) in columns) {
          if (FixedColumns.Contains(name)) {
            continue;
          }

          var value = GetColumnValue(column, i);
          if (value != null) {
            entry.Attributes[name] = value;
          }
        }

        // Parse _meta overflow column
        var metaJson = GetString(columns, "_meta", i);
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

        yield return entry;
      }
    }
  }

  /// <summary>
  /// Reads a batch of log entries from a Parquet file.
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

  private static string? GetString(Dictionary<string, DataColumn> columns, string name, int rowIndex)
  {
    if (!columns.TryGetValue(name, out var column)) {
      return null;
    }

    var data = column.Data;
    if (data is string[] strArray) {
      return strArray[rowIndex];
    }

    return null;
  }

  private static DateTime? GetDateTime(Dictionary<string, DataColumn> columns, string name, int rowIndex)
  {
    if (!columns.TryGetValue(name, out var column)) {
      return null;
    }

    var data = column.Data;
    if (data is DateTime[] dtArray) {
      return dtArray[rowIndex];
    }
    if (data is DateTimeOffset[] dtoArray) {
      return dtoArray[rowIndex].UtcDateTime;
    }

    return null;
  }

  private static int? GetNullableInt(Dictionary<string, DataColumn> columns, string name, int rowIndex)
  {
    if (!columns.TryGetValue(name, out var column)) {
      return null;
    }

    var data = column.Data;
    if (data is int?[] nullableIntArray) {
      return nullableIntArray[rowIndex];
    }
    if (data is int[] intArray) {
      return intArray[rowIndex];
    }

    return null;
  }

  private static object? GetColumnValue(DataColumn column, int rowIndex)
  {
    var data = column.Data;

    return data switch {
      string[] arr => arr[rowIndex],
      int?[] arr => arr[rowIndex],
      int[] arr => arr[rowIndex],
      long?[] arr => arr[rowIndex],
      long[] arr => arr[rowIndex],
      float?[] arr => arr[rowIndex],
      float[] arr => arr[rowIndex],
      double?[] arr => arr[rowIndex],
      double[] arr => arr[rowIndex],
      bool?[] arr => arr[rowIndex],
      bool[] arr => arr[rowIndex],
      DateTime?[] arr => arr[rowIndex],
      DateTime[] arr => arr[rowIndex],
      DateTimeOffset?[] arr => (object?)arr[rowIndex]?.UtcDateTime,
      DateTimeOffset[] arr => arr[rowIndex].UtcDateTime,
      byte[][] arr => arr[rowIndex],
      _ => null
    };
  }
}