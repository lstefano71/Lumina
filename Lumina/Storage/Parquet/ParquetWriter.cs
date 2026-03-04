using Lumina.Core.Models;

using Parquet.Data;
using Parquet.Schema;

using System.IO.Compression;
using System.Text.Json;

namespace Lumina.Storage.Parquet;

/// <summary>
/// Writes log entries to Parquet files using parquet-dotnet.
/// </summary>
public static class ParquetWriter
{
  /// <summary>
  /// Writes a batch of log entries to a Parquet file.
  /// </summary>
  /// <param name="entries">The log entries to write.</param>
  /// <param name="outputPath">The output file path.</param>
  /// <param name="maxDynamicKeys">Maximum dynamic keys before overflow.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public static async Task WriteBatchAsync(
      IReadOnlyList<LogEntry> entries,
      string outputPath,
      int maxDynamicKeys = 100,
      CancellationToken cancellationToken = default)
  {
    if (entries.Count == 0) {
      throw new ArgumentException("Entries collection cannot be empty.", nameof(entries));
    }

    // Resolve schema
    var schema = SchemaResolver.ResolveSchema(entries, maxDynamicKeys);
    var overflowKeys = SchemaResolver.GetOverflowKeys(
        entries,
        schema.Where(c => !c.IsOverflow).Select(c => c.Name).ToHashSet(),
        maxDynamicKeys);

    // Build Parquet columns/schema
    var fieldDataPairs = CollectFieldDataPairs(entries, schema, overflowKeys);
    var parquetSchema = new ParquetSchema(fieldDataPairs.Select(p => p.Field));
    var columns = fieldDataPairs.Select(p => new DataColumn(p.Field, p.Data)).ToList();

    // Ensure output directory exists
    var directory = Path.GetDirectoryName(outputPath);
    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory)) {
      Directory.CreateDirectory(directory);
    }

    // Write to Parquet file asynchronously
    await using var fileStream = new FileStream(
        outputPath,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None,
        bufferSize: 65536,
        options: FileOptions.Asynchronous);

    var parquetOptions = new global::Parquet.ParquetOptions {
      UseDictionaryEncoding = true,
      DictionaryEncodingThreshold = 0.8,
      UseDeltaBinaryPackedEncoding = true
    };

    await using var writer = await global::Parquet.ParquetWriter.CreateAsync(
        parquetSchema,
        fileStream,
        parquetOptions,
        append: false,
        cancellationToken);
    writer.CompressionMethod = global::Parquet.CompressionMethod.Zstd;
    writer.CompressionLevel = CompressionLevel.Optimal;

    using var rowGroup = writer.CreateRowGroup();

    foreach (var column in columns) {
      cancellationToken.ThrowIfCancellationRequested();
      await rowGroup.WriteColumnAsync(column);
    }
  }

  private static List<(DataField Field, Array Data)> CollectFieldDataPairs(
      IReadOnlyList<LogEntry> entries,
      IReadOnlyList<ColumnSchema> schema,
      IReadOnlySet<string> overflowKeys)
  {
    var pairs = new List<(DataField, Array)>(schema.Count);

    foreach (var column in schema) {
      pairs.Add(column.Name switch {
        "stream" => (CreateDataField("stream", typeof(string), column.IsNullable), entries.Select(e => e.Stream).ToArray()),
        "_t" => (new DateTimeDataField("_t", DateTimeFormat.DateAndTimeMicros, isAdjustedToUTC: true, unit: null, isNullable: column.IsNullable), entries.Select(e => e.Timestamp.ToUniversalTime()).ToArray()),
        "level" => (CreateDataField("level", typeof(string), column.IsNullable), entries.Select(e => e.Level).ToArray()),
        "message" => (CreateDataField("message", typeof(string), column.IsNullable), entries.Select(e => e.Message).ToArray()),
        "trace_id" => (CreateDataField("trace_id", typeof(string), column.IsNullable), entries.Select(e => e.TraceId).ToArray()),
        "span_id" => (CreateDataField("span_id", typeof(string), column.IsNullable), entries.Select(e => e.SpanId).ToArray()),
        "duration_ms" => (CreateDataField("duration_ms", typeof(int?), column.IsNullable), entries.Select(e => e.DurationMs).ToArray()),
        "_meta" => (CreateDataField("_meta", typeof(string), column.IsNullable), BuildMetaValues(entries, overflowKeys)),
        _ => BuildAttributePair(entries, column)
      });
    }

    return pairs;
  }

  private static (DataField Field, Array Data) BuildAttributePair(
      IReadOnlyList<LogEntry> entries,
      ColumnSchema column)
  {
    var key = column.Name;

    return column.Type switch {
      SchemaType.Boolean => (CreateDataField(key, typeof(bool?), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is bool v ? v : (bool?)null).ToArray()),
      SchemaType.Int32 => (CreateDataField(key, typeof(int?), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is int v ? v : (int?)null).ToArray()),
      SchemaType.Int64 => (CreateDataField(key, typeof(long?), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is long v ? v : (long?)null).ToArray()),
      SchemaType.Float => (CreateDataField(key, typeof(float?), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is float v ? v : (float?)null).ToArray()),
      SchemaType.Double => (CreateDataField(key, typeof(double?), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is double v ? v : (double?)null).ToArray()),
      SchemaType.Binary => (CreateDataField(key, typeof(byte[]), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is byte[] v ? v : null).ToArray()),
      SchemaType.Timestamp => (new DateTimeDataField(key, DateTimeFormat.DateAndTimeMicros, isAdjustedToUTC: true, unit: null, isNullable: column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value is DateTime v ? v.ToUniversalTime() : (DateTime?)null).ToArray()),
      _ => (CreateDataField(key, typeof(string), column.IsNullable), entries.Select(e =>
          e.Attributes.TryGetValue(key, out var value) && value != null ? value.ToString() : null).ToArray())
    };
  }

  private static DataField CreateDataField(string name, Type type, bool isNullable = true)
  {
    return new DataField(name, type, isNullable);
  }

  private static string?[] BuildMetaValues(IReadOnlyList<LogEntry> entries, IReadOnlySet<string> overflowKeys)
  {
    var values = new string?[entries.Count];

    for (int i = 0; i < entries.Count; i++) {
      var overflow = entries[i].Attributes
          .Where(a => overflowKeys.Contains(a.Key))
          .ToDictionary(a => a.Key, a => a.Value);

      values[i] = overflow.Count == 0 ? null : JsonSerializer.Serialize(overflow);
    }

    return values;
  }

  /// <summary>
  /// Generates an idempotent Parquet file name.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="startTime">Start time of the data.</param>
  /// <param name="endTime">End time of the data.</param>
  /// <param name="hash">Content hash for uniqueness.</param>
  public static string GenerateFileName(string stream, DateTime startTime, DateTime endTime, string? hash = null)
  {
    var startTimeStr = startTime.ToString("yyyyMMdd_HHmmss");
    var endTimeStr = endTime.ToString("yyyyMMdd_HHmmss");

    if (!string.IsNullOrEmpty(hash)) {
      return $"{stream}_{startTimeStr}_{endTimeStr}_{hash}.parquet";
    }

    return $"{stream}_{startTimeStr}_{endTimeStr}.parquet";
  }
}