namespace Lumina.Storage.Catalog;

/// <summary>
/// Configuration options for the catalog system.
/// </summary>
public sealed class CatalogOptions
{
  /// <summary>
  /// Gets the directory where catalog.json is stored.
  /// Default is "data/catalog".
  /// </summary>
  public string CatalogDirectory { get; init; } = "data/catalog";

  /// <summary>
  /// Gets a value indicating whether to rebuild catalog on startup if corrupted.
  /// Default is true.
  /// </summary>
  public bool EnableAutoRebuild { get; init; } = true;

  /// <summary>
  /// Gets a value indicating whether to run garbage collection on startup.
  /// Default is true.
  /// </summary>
  public bool EnableStartupGc { get; init; } = true;

  /// <summary>
  /// Gets the catalog file name.
  /// </summary>
  public string CatalogFileName { get; init; } = "catalog.json";

  /// <summary>
  /// Gets the temporary catalog file name for safe-write pattern.
  /// </summary>
  public string CatalogTempFileName { get; init; } = "catalog.tmp.json";
}