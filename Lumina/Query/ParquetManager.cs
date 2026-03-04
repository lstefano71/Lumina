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
  /// Discovers all unique stream names from L1 and L2 directories.
  /// </summary>
  /// <returns>List of unique stream names.</returns>
  public IReadOnlyList<string> DiscoverAllStreams()
  {
    var streams = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    // Scan L1 directory for streams
    if (Directory.Exists(_settings.L1Directory)) {
      foreach (var dir in Directory.GetDirectories(_settings.L1Directory)) {
        var streamName = Path.GetFileName(dir);
        if (!string.IsNullOrEmpty(streamName)) {
          streams.Add(streamName);
        }
      }
    }

    // Scan L2 directory for streams
    if (Directory.Exists(_settings.L2Directory)) {
      foreach (var dir in Directory.GetDirectories(_settings.L2Directory)) {
        var streamName = Path.GetFileName(dir);
        if (!string.IsNullOrEmpty(streamName)) {
          streams.Add(streamName);
        }
      }
    }

    _logger.LogDebug("Discovered {Count} streams from L1/L2 directories", streams.Count);
    return streams.OrderBy(s => s).ToList();
  }

  /// <summary>
  /// Gets stream mappings for all discovered streams.
  /// </summary>
  /// <returns>List of stream table mappings.</returns>
  public IReadOnlyList<StreamTableMapping> GetStreamMappings()
  {
    var streams = DiscoverAllStreams();
    var mappings = new List<StreamTableMapping>();

    foreach (var stream in streams) {
      var files = GetStreamFiles(stream);
      mappings.Add(new StreamTableMapping {
        StreamName = stream,
        ParquetFiles = files
      });
    }

    return mappings;
  }

  /// <summary>
  /// Gets a single stream mapping by name.
  /// </summary>
  /// <param name="streamName">The stream name.</param>
  /// <returns>The stream mapping, or null if not found.</returns>
  public StreamTableMapping? GetStreamMapping(string streamName)
  {
    var files = GetStreamFiles(streamName);

    // Return null if stream has no files and directories don't exist
    if (files.Count == 0) {
      var l1Dir = Path.Combine(_settings.L1Directory, streamName);
      var l2Dir = Path.Combine(_settings.L2Directory, streamName);

      if (!Directory.Exists(l1Dir) && !Directory.Exists(l2Dir)) {
        return null;
      }
    }

    return new StreamTableMapping {
      StreamName = streamName,
      ParquetFiles = files
    };
  }

  /// <summary>
  /// Gets schema information for a stream.
  /// </summary>
  /// <param name="streamName">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Schema information, or null if stream not found.</returns>
  public async Task<StreamSchemaInfo?> GetStreamSchemaAsync(string streamName, CancellationToken cancellationToken = default)
  {
    var mapping = GetStreamMapping(streamName);
    if (mapping == null) {
      return null;
    }

    var files = mapping.ParquetFiles;
    if (files.Count == 0) {
      return new StreamSchemaInfo {
        StreamName = streamName,
        Columns = Array.Empty<ColumnInfo>(),
        FileCount = 0,
        TotalSizeBytes = 0
      };
    }

    // Get file stats
    long totalSize = 0;
    DateTime? minTimestamp = null;
    DateTime? maxTimestamp = null;

    foreach (var file in files) {
      var fileInfo = new FileInfo(file);
      totalSize += fileInfo.Length;
    }

    // We'll return basic info; actual schema inference would require reading a Parquet file
    // This can be enhanced later to use ParquetReader to infer schema
    return new StreamSchemaInfo {
      StreamName = streamName,
      Columns = await GetColumnsFromParquetAsync(files[0], cancellationToken),
      FileCount = files.Count,
      TotalSizeBytes = totalSize,
      MinTimestamp = minTimestamp,
      MaxTimestamp = maxTimestamp
    };
  }

  /// <summary>
  /// Gets column information from a Parquet file.
  /// </summary>
  private async Task<IReadOnlyList<ColumnInfo>> GetColumnsFromParquetAsync(string filePath, CancellationToken cancellationToken)
  {
    try {
      await using var stream = File.OpenRead(filePath);
      using var reader = await global::Parquet.ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);

      var dataFields = reader.Schema.GetDataFields();
      var columns = new List<ColumnInfo>();

      foreach (var field in dataFields) {
        columns.Add(new ColumnInfo {
          Name = field.Name,
          Type = GetTypeName(field),
          IsNullable = field.IsNullable
        });
      }

      return columns;
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Failed to read schema from Parquet file: {FilePath}", filePath);
      return Array.Empty<ColumnInfo>();
    }
  }

  /// <summary>
  /// Gets a human-readable type name from a DataField.
  /// </summary>
  private static string GetTypeName(Parquet.Schema.DataField field)
  {
    // Get the type name from the field's ClrType
    var typeName = field.ClrType?.Name ?? "UNKNOWN";

    return typeName switch {
      "String" => "STRING",
      "DateTime" => "TIMESTAMP",
      "DateTimeOffset" => "TIMESTAMP",
      "Int32" => "INT32",
      "Int64" => "INT64",
      "Single" => "FLOAT",
      "Double" => "DOUBLE",
      "Boolean" => "BOOLEAN",
      "Byte[]" => "BINARY",
      _ => typeName.ToUpperInvariant()
    };
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