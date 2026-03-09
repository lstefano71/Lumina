namespace Lumina.Storage.Parquet;

/// <summary>
/// Reads statistics from Parquet file metadata for efficient time-range queries.
/// </summary>
public static class ParquetStatisticsReader
{
  /// <summary>
  /// Extracts Min/Max statistics for the _t column from a Parquet file.
  /// </summary>
  /// <param name="filePath">Path to the Parquet file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Tuple of (MinTime, MaxTime) or null if unavailable.</returns>
  public static async Task<(DateTime MinTime, DateTime MaxTime)?> ExtractTimeBoundsAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    try {
      await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
      // NOTE: We do not need to read data, just metadata
      using var reader = await global::Parquet.ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);

      // Find the _t column in the schema
      var dataFields = reader.Schema.GetDataFields();
      var timeField = dataFields.FirstOrDefault(f => f.Name == "_t");

      if (timeField == null) {
        return null;
      }

      DateTime? minTime = null;
      DateTime? maxTime = null;

      for (int i = 0; i < reader.RowGroupCount; i++) {
        cancellationToken.ThrowIfCancellationRequested();
        using var rowGroupReader = reader.OpenRowGroupReader(i);
        var stats = rowGroupReader.GetStatistics(timeField);
        if (stats == null) continue;

        DateTime? sMin = stats.MinValue switch {
          DateTimeOffset dto => dto.UtcDateTime,
          DateTime dt => dt,
          _ => null
        };
        DateTime? sMax = stats.MaxValue switch {
          DateTimeOffset dto => dto.UtcDateTime,
          DateTime dt => dt,
          _ => null
        };

        if (sMin != null && (minTime == null || sMin < minTime)) minTime = sMin;
        if (sMax != null && (maxTime == null || sMax > maxTime)) maxTime = sMax;
      }

      if (minTime.HasValue && maxTime.HasValue) {
        return (minTime.Value, maxTime.Value);
      }

      return null;
    } catch (Exception) {
      // If we can't read the file, return null
      return null;
    }
  }

  /// <summary>
  /// Gets the row count from a Parquet file.
  /// </summary>
  /// <param name="filePath">Path to the Parquet file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Row count, or 0 if unable to read.</returns>
  public static async Task<long> GetRowCountAsync(string filePath, CancellationToken cancellationToken = default)
  {
    try {
      await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
      using var reader = await global::Parquet.ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);
      return reader.RowGroups.Sum(rg => rg.RowCount);
    } catch {
      return 0;
    }
  }

  /// <summary>
  /// Reads file-level custom metadata from a Parquet file.
  /// </summary>
  /// <param name="filePath">Path to the Parquet file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Custom metadata dictionary, or an empty dictionary if unavailable.</returns>
  public static async Task<IReadOnlyDictionary<string, string>> ReadCustomMetadataAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    try {
      await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
      using var reader = await global::Parquet.ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);
      return reader.CustomMetadata ?? new Dictionary<string, string>();
    } catch {
      return new Dictionary<string, string>();
    }
  }
}
