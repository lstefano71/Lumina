using System.Text.Json.Serialization;

namespace Lumina.Storage.Catalog;

/// <summary>
/// Represents the complete catalog state.
/// </summary>
public sealed class StreamCatalog
{
  /// <summary>
  /// Gets or sets the list of catalog entries.
  /// </summary>
  [JsonPropertyName("entries")]
  public List<CatalogEntry> Entries { get; set; } = new();

  /// <summary>
  /// Gets or sets the last modification timestamp.
  /// </summary>
  [JsonPropertyName("lastModified")]
  public DateTime LastModified { get; set; } = DateTime.UtcNow;

  /// <summary>
  /// Gets or sets the catalog version for optimistic concurrency.
  /// </summary>
  [JsonPropertyName("version")]
  public long Version { get; set; } = 1;
}