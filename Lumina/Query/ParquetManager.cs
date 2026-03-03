using Lumina.Core.Configuration;

namespace Lumina.Query;

/// <summary>
/// Manages Parquet file discovery and organization.
/// </summary>
public sealed class ParquetManager
{
  private readonly CompactionSettings _settings;
  private readonly ILogger<ParquetManager> _logger;

  public ParquetManager(CompactionSettings settings, ILogger<ParquetManager> logger)
  {
    _settings = settings;
    _logger = logger;
  }

  /// <summary>
  /// Gets all L1 Parquet files.
  /// </summary>
  /// <returns>List of file paths.</returns>
  public IReadOnlyList<string> GetL1Files()
  {
    return GetParquetFiles(_settings.L1Directory);
  }

  /// <summary>
  /// Gets all L2 Parquet files.
  /// </summary>
  /// <returns>List of file paths.</returns>
  public IReadOnlyList<string> GetL2Files()
  {
    return GetParquetFiles(_settings.L2Directory);
  }

  /// <summary>
  /// Gets all Parquet files for a specific stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>List of file paths.</returns>
  public IReadOnlyList<string> GetStreamFiles(string stream)
  {
    var files = new List<string>();

    // L1 files
    var l1Dir = Path.Combine(_settings.L1Directory, stream);
    if (Directory.Exists(l1Dir)) {
      files.AddRange(Directory.GetFiles(l1Dir, "*.parquet"));
    }

    // L2 files
    var l2Dir = Path.Combine(_settings.L2Directory, stream);
    if (Directory.Exists(l2Dir)) {
      files.AddRange(Directory.GetFiles(l2Dir, "*.parquet"));
    }

    return files.OrderBy(f => f).ToList();
  }

  /// <summary>
  /// Gets Parquet files within a time range.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="start">Start time.</param>
  /// <param name="end">End time.</param>
  /// <returns>List of file paths.</returns>
  public IReadOnlyList<string> GetFilesInRange(string stream, DateTime start, DateTime end)
  {
    var files = GetStreamFiles(stream);
    var result = new List<string>();

    foreach (var file in files) {
      // Parse timestamps from filename (format: stream_start_end.parquet)
      var fileName = Path.GetFileNameWithoutExtension(file);
      var parts = fileName.Split('_');

      if (parts.Length >= 3) {
        // Try to parse start time from filename
        if (DateTime.TryParseExact(parts[1], "yyyyMMdd HHmmss", null,
            System.Globalization.DateTimeStyles.None, out var fileStart)) {
          if (fileStart <= end && fileStart >= start.AddHours(-1)) {
            result.Add(file);
          }
        }
      }
    }

    return result;
  }

  /// <summary>
  /// Gets the total size of Parquet files.
  /// </summary>
  /// <returns>Total size in bytes.</returns>
  public long GetTotalSize()
  {
    long total = 0;

    if (Directory.Exists(_settings.L1Directory)) {
      total += Directory.GetFiles(_settings.L1Directory, "*.parquet", SearchOption.AllDirectories)
          .Sum(f => new FileInfo(f).Length);
    }

    if (Directory.Exists(_settings.L2Directory)) {
      total += Directory.GetFiles(_settings.L2Directory, "*.parquet", SearchOption.AllDirectories)
          .Sum(f => new FileInfo(f).Length);
    }

    return total;
  }

  private static IReadOnlyList<string> GetParquetFiles(string directory)
  {
    if (!Directory.Exists(directory)) {
      return Array.Empty<string>();
    }

    return Directory.GetFiles(directory, "*.parquet", SearchOption.AllDirectories)
        .OrderBy(f => f)
        .ToList();
  }
}