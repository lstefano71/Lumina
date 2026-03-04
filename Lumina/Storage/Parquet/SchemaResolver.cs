using Lumina.Core.Models;

namespace Lumina.Storage.Parquet;

/// <summary>
/// Represents a resolved schema type for Parquet columns.
/// </summary>
public enum SchemaType
{
  Null,
  Boolean,
  Int32,
  Int64,
  Float,
  Double,
  String,
  Binary,
  Timestamp,
  Json
}

/// <summary>
/// Represents a resolved column schema.
/// </summary>
public sealed class ColumnSchema
{
  public string Name { get; init; } = string.Empty;
  public SchemaType Type { get; init; }
  public bool IsNullable { get; init; } = true;
  public bool IsOverflow { get; init; } = false;
}

/// <summary>
/// Resolves union schemas from multiple log entries for Parquet writing.
/// Handles type promotion and overflow key packing.
/// </summary>
public static class SchemaResolver
{
  /// <summary>
  /// Resolves a unified schema from a collection of log entries.
  /// </summary>
  /// <param name="entries">The log entries to analyze.</param>
  /// <param name="maxDynamicKeys">Maximum number of dynamic keys before overflow.</param>
  /// <returns>The resolved column schemas.</returns>
  public static IReadOnlyList<ColumnSchema> ResolveSchema(
      IReadOnlyList<LogEntry> entries,
      int maxDynamicKeys = 100)
  {
    if (entries.Count == 0) {
      return Array.Empty<ColumnSchema>();
    }

    var columns = new Dictionary<string, SchemaType>();
    var keyCounts = new Dictionary<string, int>();

    // Fixed columns
    columns["stream"] = SchemaType.String;
    columns["_t"] = SchemaType.Timestamp;
    columns["level"] = SchemaType.String;
    columns["message"] = SchemaType.String;
    columns["trace_id"] = SchemaType.String;
    columns["span_id"] = SchemaType.String;
    columns["duration_ms"] = SchemaType.Int32;

    // Collect all attribute keys and their types
    foreach (var entry in entries) {
      foreach (var attr in entry.Attributes) {
        var key = attr.Key;

        if (!keyCounts.ContainsKey(key)) {
          keyCounts[key] = 0;
        }
        keyCounts[key]++;

        var valueType = InferType(attr.Value);

        if (columns.TryGetValue(key, out var existingType)) {
          // Promote type if needed
          columns[key] = PromoteType(existingType, valueType);
        } else {
          columns[key] = valueType;
        }
      }
    }

    // Determine overflow keys
    var overflowKeys = keyCounts
        .Where(k => k.Value < entries.Count * 0.1 || keyCounts.Count > maxDynamicKeys)
        .Select(k => k.Key)
        .ToHashSet();

    // Build final schema
    var result = new List<ColumnSchema>();

    // Add fixed columns
    result.Add(new ColumnSchema { Name = "stream", Type = SchemaType.String, IsNullable = false });
    result.Add(new ColumnSchema { Name = "_t", Type = SchemaType.Timestamp, IsNullable = false });
    result.Add(new ColumnSchema { Name = "level", Type = SchemaType.String, IsNullable = false });
    result.Add(new ColumnSchema { Name = "message", Type = SchemaType.String, IsNullable = false });
    result.Add(new ColumnSchema { Name = "trace_id", Type = SchemaType.String });
    result.Add(new ColumnSchema { Name = "span_id", Type = SchemaType.String });
    result.Add(new ColumnSchema { Name = "duration_ms", Type = SchemaType.Int32 });

    // Add attribute columns (excluding overflow keys)
    foreach (var col in columns) {
      if (IsFixedColumn(col.Key))
        continue;

      if (overflowKeys.Contains(col.Key))
        continue;

      result.Add(new ColumnSchema {
        Name = col.Key,
        Type = col.Value,
        IsNullable = true
      });
    }

    // Add overflow meta column if needed
    if (overflowKeys.Count > 0) {
      result.Add(new ColumnSchema {
        Name = "_meta",
        Type = SchemaType.Json,
        IsNullable = true,
        IsOverflow = true
      });
    }

    return result;
  }

  /// <summary>
  /// Gets the overflow keys that should be packed into the _meta column.
  /// </summary>
  public static IReadOnlySet<string> GetOverflowKeys(
      IReadOnlyList<LogEntry> entries,
      IReadOnlySet<string> schemaKeys,
      int maxDynamicKeys = 100)
  {
    var keyCounts = new Dictionary<string, int>();

    foreach (var entry in entries) {
      foreach (var attr in entry.Attributes) {
        if (!keyCounts.ContainsKey(attr.Key)) {
          keyCounts[attr.Key] = 0;
        }
        keyCounts[attr.Key]++;
      }
    }

    return keyCounts
        .Where(k => !schemaKeys.Contains(k.Key) ||
                    k.Value < entries.Count * 0.1 ||
                    keyCounts.Count > maxDynamicKeys)
        .Select(k => k.Key)
        .ToHashSet();
  }

  /// <summary>
  /// Infers the schema type from a value.
  /// </summary>
  private static SchemaType InferType(object? value)
  {
    return value switch {
      null => SchemaType.Null,
      bool => SchemaType.Boolean,
      int => SchemaType.Int32,
      long => SchemaType.Int64,
      float => SchemaType.Float,
      double => SchemaType.Double,
      DateTime => SchemaType.Timestamp,
      DateTimeOffset => SchemaType.Timestamp,
      string => SchemaType.String,
      byte[] => SchemaType.Binary,
      Dictionary<string, object?> => SchemaType.Json,
      _ => SchemaType.String
    };
  }

  /// <summary>
  /// Promotes a type to accommodate both existing and new values.
  /// </summary>
  private static SchemaType PromoteType(SchemaType current, SchemaType newValue)
  {
    // Type promotion matrix
    return (current, newValue) switch {
      // Same type - no promotion needed
      (var a, var b) when a == b => a,

      // Null can be promoted to any type
      (SchemaType.Null, var b) => b,
      (var a, SchemaType.Null) => a,

      // Int32 -> Int64 or Double
      (SchemaType.Int32, SchemaType.Int64) => SchemaType.Int64,
      (SchemaType.Int64, SchemaType.Int32) => SchemaType.Int64,
      (SchemaType.Int32, SchemaType.Float) => SchemaType.Double,
      (SchemaType.Float, SchemaType.Int32) => SchemaType.Double,
      (SchemaType.Int32, SchemaType.Double) => SchemaType.Double,
      (SchemaType.Double, SchemaType.Int32) => SchemaType.Double,

      // Int64 -> Double
      (SchemaType.Int64, SchemaType.Double) => SchemaType.Double,
      (SchemaType.Double, SchemaType.Int64) => SchemaType.Double,
      (SchemaType.Int64, SchemaType.Float) => SchemaType.Double,
      (SchemaType.Float, SchemaType.Int64) => SchemaType.Double,

      // Float -> Double
      (SchemaType.Float, SchemaType.Double) => SchemaType.Double,
      (SchemaType.Double, SchemaType.Float) => SchemaType.Double,

      // Everything else promotes to String (most flexible)
      _ => SchemaType.String
    };
  }

  private static bool IsFixedColumn(string name)
  {
    return name is "stream" or "_t" or "level" or "message" or "trace_id" or "span_id" or "duration_ms";
  }
}