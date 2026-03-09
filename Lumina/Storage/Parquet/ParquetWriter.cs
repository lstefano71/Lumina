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
        "_s" => (CreateDataField("_s", typeof(string), column.IsNullable), entries.Select(e => e.Stream).ToArray()),
        "_t" => (new DataField<DateTime>("_t", column.IsNullable), entries.Select(e => e.Timestamp.ToUniversalTime()).ToArray()),
        "_l" => (CreateDataField("_l", typeof(string), column.IsNullable), entries.Select(e => e.Level).ToArray()),
        "_m" => (CreateDataField("_m", typeof(string), column.IsNullable), entries.Select(e => e.Message).ToArray()),
        "_traceid" => (CreateDataField("_traceid", typeof(string), column.IsNullable), entries.Select(e => e.TraceId).ToArray()),
        "_spanid" => (CreateDataField("_spanid", typeof(string), column.IsNullable), entries.Select(e => e.SpanId).ToArray()),
        "_duration_ms" => (CreateDataField("_duration_ms", typeof(int?), column.IsNullable), entries.Select(e => e.DurationMs).ToArray()),
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
      SchemaType.Timestamp => (new DataField<DateTime?>(key, column.IsNullable), entries.Select(e =>
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
  /// Writes log entries from an async source of chunks, one Parquet row group per chunk.
  /// Only one chunk is held in memory at a time, bounding memory usage.
  /// </summary>
  /// <param name="chunks">Async enumerable of entry batches; each batch becomes one row group.</param>
  /// <param name="outputPath">The output file path.</param>
  /// <param name="maxDynamicKeys">Maximum dynamic keys before overflow.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Total number of entries written.</returns>
  public static async Task<int> WriteChunkedAsync(
      IAsyncEnumerable<IReadOnlyList<LogEntry>> chunks,
      string outputPath,
      int maxDynamicKeys = 100,
      IReadOnlyDictionary<string, string>? fileMetadata = null,
      CancellationToken cancellationToken = default)
  {
    var directory = Path.GetDirectoryName(outputPath);
    if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory)) {
      Directory.CreateDirectory(directory);
    }

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

    global::Parquet.ParquetWriter? writer = null;
    ParquetSchema? masterParquetSchema = null;
    List<DataField>? masterFields = null;
    IReadOnlyList<ColumnSchema>? masterSchema = null;
    IReadOnlySet<string>? masterSchemaNames = null;
    var totalRows = 0;

    try {
      await foreach (var chunk in chunks.WithCancellation(cancellationToken)) {
        if (chunk.Count == 0) continue;

        List<DataColumn> columns;

        if (writer == null) {
          // First chunk: establish master schema
          masterSchema = SchemaResolver.ResolveSchema(chunk, maxDynamicKeys);
          var overflowKeys = SchemaResolver.GetOverflowKeys(
              chunk,
              masterSchema.Where(c => !c.IsOverflow).Select(c => c.Name).ToHashSet(),
              maxDynamicKeys);

          var fieldDataPairs = CollectFieldDataPairs(chunk, masterSchema, overflowKeys);
          masterFields = fieldDataPairs.Select(p => p.Field).ToList();
          masterParquetSchema = new ParquetSchema(masterFields);
          masterSchemaNames = masterFields.Select(f => f.Name).ToHashSet();
          columns = fieldDataPairs.Select(p => new DataColumn(p.Field, p.Data)).ToList();

          writer = await global::Parquet.ParquetWriter.CreateAsync(
              masterParquetSchema, fileStream, parquetOptions, append: false, cancellationToken);
          writer.CompressionMethod = global::Parquet.CompressionMethod.Zstd;
          writer.CompressionLevel = CompressionLevel.Optimal;
          if (fileMetadata != null)
            writer.CustomMetadata = new Dictionary<string, string>(fileMetadata);
        } else {
          // Subsequent chunks: align columns to master schema order
          var chunkSchema = SchemaResolver.ResolveSchema(chunk, maxDynamicKeys);
          var chunkPromotedNames = chunkSchema
              .Where(c => !c.IsOverflow)
              .Select(c => c.Name)
              .ToHashSet();

          // Keys present in this chunk but NOT in master schema → overflow to _meta
          var extraOverflowKeys = chunkPromotedNames
              .Where(k => !masterSchemaNames!.Contains(k) && !IsFixedColumn(k))
              .ToHashSet();

          var baseOverflowKeys = SchemaResolver.GetOverflowKeys(
              chunk,
              chunkPromotedNames,
              maxDynamicKeys);

          // Merge extra overflow keys with computed overflow
          var combinedOverflowKeys = new HashSet<string>(baseOverflowKeys);
          combinedOverflowKeys.UnionWith(extraOverflowKeys);

          // Build a lookup of chunk field data pairs by name
          var chunkPairs = CollectFieldDataPairs(chunk, chunkSchema, combinedOverflowKeys);
          var chunkPairMap = new Dictionary<string, (DataField Field, Array Data)>();
          foreach (var pair in chunkPairs) {
            chunkPairMap[pair.Field.Name] = pair;
          }

          // Emit columns in master schema order
          columns = new List<DataColumn>(masterFields!.Count);
          foreach (var masterField in masterFields) {
            if (chunkPairMap.TryGetValue(masterField.Name, out var pair)) {
              // Use master field definition to keep schema consistent
              columns.Add(new DataColumn(masterField, pair.Data));
            } else {
              // Column missing in this chunk → null-fill
              columns.Add(CreateNullColumn(masterField, chunk.Count));
            }
          }
        }

        using var rowGroup = writer.CreateRowGroup();
        foreach (var column in columns) {
          cancellationToken.ThrowIfCancellationRequested();
          await rowGroup.WriteColumnAsync(column);
        }

        totalRows += chunk.Count;
      }
    } finally {
      if (writer != null) {
        await writer.DisposeAsync();
      }
    }

    return totalRows;
  }

  /// <summary>
  /// Creates a null-filled DataColumn for a field, used when a chunk lacks
  /// a column present in the master schema.
  /// </summary>
  private static DataColumn CreateNullColumn(DataField field, int rowCount)
  {
    if (field.ClrType == typeof(string))
      return new DataColumn(field, new string?[rowCount]);
    if (field.ClrType == typeof(bool?))
      return new DataColumn(field, new bool?[rowCount]);
    if (field.ClrType == typeof(int?))
      return new DataColumn(field, new int?[rowCount]);
    if (field.ClrType == typeof(long?))
      return new DataColumn(field, new long?[rowCount]);
    if (field.ClrType == typeof(float?))
      return new DataColumn(field, new float?[rowCount]);
    if (field.ClrType == typeof(double?))
      return new DataColumn(field, new double?[rowCount]);
    if (field.ClrType == typeof(DateTime) || field.ClrType == typeof(DateTime?))
      return new DataColumn(field, new DateTime?[rowCount]);
    if (field.ClrType == typeof(byte[]))
      return new DataColumn(field, new byte[]?[rowCount]);

    // Fallback: treat as nullable string
    return new DataColumn(field, new string?[rowCount]);
  }

  private static bool IsFixedColumn(string name) =>
      name is "_s" or "_t" or "_l" or "_m" or "_traceid" or "_spanid" or "_duration_ms" or "_meta";

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