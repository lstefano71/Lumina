using Apache.Arrow;
using Apache.Arrow.Types;
using ParquetSharp.Arrow;

using Lumina.Core.Models;

using System.Text.Json;

namespace Lumina.Storage.Parquet;

/// <summary>
/// Writes log entries to Parquet files using Apache Arrow.
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
  public static Task WriteBatchAsync(
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

    // Build Arrow arrays
    var arrays = BuildArrowArrays(entries, schema, overflowKeys);

    // Create Arrow schema
    var arrowSchema = CreateArrowSchema(schema);

    // Create record batch
    var recordBatch = new RecordBatch(arrowSchema, arrays, entries.Count);

    // Ensure output directory exists
    var directory = Path.GetDirectoryName(outputPath);
    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory)) {
      Directory.CreateDirectory(directory);
    }

    // Write to Parquet file
    using var writer = new FileWriter(outputPath, arrowSchema);
    writer.WriteRecordBatch(recordBatch);
    writer.Close();

    return Task.CompletedTask;
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

  private static List<IArrowArray> BuildArrowArrays(
      IReadOnlyList<LogEntry> entries,
      IReadOnlyList<ColumnSchema> schema,
      IReadOnlySet<string> overflowKeys)
  {
    var arrays = new List<IArrowArray>();

    foreach (var column in schema) {
      var array = column.Name switch {
        "stream" => BuildStringArray(entries.Select(e => e.Stream).ToList()),
        "timestamp" => BuildTimestampArray(entries.Select(e => e.Timestamp).ToList()),
        "level" => BuildStringArray(entries.Select(e => e.Level).ToList()),
        "message" => BuildStringArray(entries.Select(e => e.Message).ToList()),
        "trace_id" => BuildNullableStringArray(entries.Select(e => e.TraceId).ToList()),
        "span_id" => BuildNullableStringArray(entries.Select(e => e.SpanId).ToList()),
        "duration_ms" => BuildNullableIntArray(entries.Select(e => e.DurationMs).ToList()),
        "_meta" => BuildMetaArray(entries, overflowKeys),
        _ => BuildAttributeArray(entries, column.Name, column.Type)
      };

      arrays.Add(array);
    }

    return arrays;
  }

  private static StringArray BuildStringArray(IReadOnlyList<string> values)
  {
    var builder = new StringArray.Builder();
    builder.Reserve(values.Count);

    foreach (var value in values) {
      builder.Append(value);
    }

    return builder.Build();
  }

  private static StringArray BuildNullableStringArray(IReadOnlyList<string?> values)
  {
    var builder = new StringArray.Builder();
    builder.Reserve(values.Count);

    foreach (var value in values) {
      if (value == null) {
        builder.AppendNull();
      } else {
        builder.Append(value);
      }
    }

    return builder.Build();
  }

  private static TimestampArray BuildTimestampArray(IReadOnlyList<DateTime> values)
  {
    var builder = new TimestampArray.Builder(TimeUnit.Microsecond);
    builder.Reserve(values.Count);

    foreach (var value in values) {
      var dto = new DateTimeOffset(value.ToUniversalTime());
      builder.Append(dto);
    }

    return builder.Build();
  }

  private static Int32Array BuildNullableIntArray(IReadOnlyList<int?> values)
  {
    var builder = new Int32Array.Builder();
    builder.Reserve(values.Count);

    foreach (var value in values) {
      if (value.HasValue) {
        builder.Append(value.Value);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static StringArray BuildMetaArray(IReadOnlyList<LogEntry> entries, IReadOnlySet<string> overflowKeys)
  {
    var builder = new StringArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      var overflow = entry.Attributes
          .Where(a => overflowKeys.Contains(a.Key))
          .ToDictionary(a => a.Key, a => a.Value);

      if (overflow.Count == 0) {
        builder.AppendNull();
      } else {
        var json = JsonSerializer.Serialize(overflow);
        builder.Append(json);
      }
    }

    return builder.Build();
  }

  private static IArrowArray BuildAttributeArray(
      IReadOnlyList<LogEntry> entries,
      string key,
      SchemaType type)
  {
    return type switch {
      SchemaType.Boolean => BuildBoolAttributeArray(entries, key),
      SchemaType.Int32 => BuildInt32AttributeArray(entries, key),
      SchemaType.Int64 => BuildInt64AttributeArray(entries, key),
      SchemaType.Float => BuildFloatAttributeArray(entries, key),
      SchemaType.Double => BuildDoubleAttributeArray(entries, key),
      SchemaType.String => BuildStringAttributeArray(entries, key),
      SchemaType.Binary => BuildBinaryAttributeArray(entries, key),
      SchemaType.Timestamp => BuildTimestampAttributeArray(entries, key),
      _ => BuildStringAttributeArray(entries, key)
    };
  }

  private static BooleanArray BuildBoolAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new BooleanArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is bool boolValue) {
        builder.Append(boolValue);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static Int32Array BuildInt32AttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new Int32Array.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is int intValue) {
        builder.Append(intValue);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static Int64Array BuildInt64AttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new Int64Array.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is long longValue) {
        builder.Append(longValue);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static FloatArray BuildFloatAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new FloatArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is float floatValue) {
        builder.Append(floatValue);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static DoubleArray BuildDoubleAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new DoubleArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is double doubleValue) {
        builder.Append(doubleValue);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static StringArray BuildStringAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new StringArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value != null) {
        builder.Append(value.ToString() ?? string.Empty);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static BinaryArray BuildBinaryAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new BinaryArray.Builder();
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is byte[] bytes) {
        builder.Append(bytes);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static TimestampArray BuildTimestampAttributeArray(IReadOnlyList<LogEntry> entries, string key)
  {
    var builder = new TimestampArray.Builder(TimeUnit.Microsecond);
    builder.Reserve(entries.Count);

    foreach (var entry in entries) {
      if (entry.Attributes.TryGetValue(key, out var value) && value is DateTime dt) {
        var dto = new DateTimeOffset(dt.ToUniversalTime());
        builder.Append(dto);
      } else {
        builder.AppendNull();
      }
    }

    return builder.Build();
  }

  private static Schema CreateArrowSchema(IReadOnlyList<ColumnSchema> columns)
  {
    var fields = new List<Field>();

    foreach (var column in columns) {
      IArrowType fieldType = column.Type switch {
        SchemaType.Boolean => BooleanType.Default,
        SchemaType.Int32 => Int32Type.Default,
        SchemaType.Int64 => Int64Type.Default,
        SchemaType.Float => FloatType.Default,
        SchemaType.Double => DoubleType.Default,
        SchemaType.String => StringType.Default,
        SchemaType.Binary => BinaryType.Default,
        SchemaType.Timestamp => new TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc),
        SchemaType.Json => StringType.Default,
        _ => StringType.Default
      };

      fields.Add(new Field(column.Name, fieldType, column.IsNullable));
    }

    return new Schema(fields, null);
  }
}