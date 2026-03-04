using System.Text.Json;
using System.Text.Json.Serialization;

namespace Lumina.Storage.Catalog;

/// <summary>
/// Represents an active Parquet file in the stream catalog.
/// </summary>
public sealed class CatalogEntry
{
  /// <summary>
  /// Gets the stream name this file belongs to.
  /// </summary>
  [JsonPropertyName("streamName")]
  public required string StreamName { get; init; }

  /// <summary>
  /// Gets the minimum timestamp in this file (from _t column statistics).
  /// </summary>
  [JsonPropertyName("minTime")]
  public DateTime MinTime { get; init; }

  /// <summary>
  /// Gets the maximum timestamp in this file (from _t column statistics).
  /// </summary>
  [JsonPropertyName("maxTime")]
  public DateTime MaxTime { get; init; }

  /// <summary>
  /// Gets the absolute file path.
  /// </summary>
  [JsonPropertyName("filePath")]
  public required string FilePath { get; init; }

  /// <summary>
  /// Gets the storage level: L1 (uncompacted) or L2 (daily consolidated).
  /// </summary>
  [JsonPropertyName("level")]
  public StorageLevel Level { get; init; }

  /// <summary>
  /// Gets the number of log entries in this file.
  /// </summary>
  [JsonPropertyName("rowCount")]
  public long RowCount { get; init; }

  /// <summary>
  /// Gets the file size in bytes.
  /// </summary>
  [JsonPropertyName("fileSizeBytes")]
  public long FileSizeBytes { get; init; }

  /// <summary>
  /// Gets the timestamp when this entry was added to the catalog.
  /// </summary>
  [JsonPropertyName("addedAt")]
  public DateTime AddedAt { get; init; }

  /// <summary>
  /// Gets the compaction tier (for future L3+ support).
  /// </summary>
  [JsonPropertyName("compactionTier")]
  public int CompactionTier { get; init; } = 1;
}

/// <summary>
/// Storage level for catalog entries.
/// </summary>
[JsonConverter(typeof(StorageLevelJsonConverter))]
public enum StorageLevel
{
  /// <summary>
  /// L1: Uncompacted Parquet files from WAL conversion.
  /// </summary>
  L1 = 1,

  /// <summary>
  /// L2: Daily consolidated Parquet files.
  /// </summary>
  L2 = 2
}

/// <summary>
/// JSON converter for StorageLevel enum.
/// </summary>
public sealed class StorageLevelJsonConverter : JsonConverter<StorageLevel>
{
  public override StorageLevel Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
  {
    var value = reader.GetString();
    return value?.ToUpperInvariant() switch {
      "L1" => StorageLevel.L1,
      "L2" => StorageLevel.L2,
      _ => (StorageLevel)reader.GetInt32()
    };
  }

  public override void Write(Utf8JsonWriter writer, StorageLevel value, JsonSerializerOptions options)
  {
    writer.WriteStringValue(value.ToString());
  }
}