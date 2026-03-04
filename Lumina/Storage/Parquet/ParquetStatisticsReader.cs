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
        // Fall back to legacy "timestamp" column for backward compatibility
        timeField = dataFields.FirstOrDefault(f => f.Name == "timestamp");
      }

      if (timeField == null) {
        return null;
      }

      int fieldIndex = Array.IndexOf(dataFields.ToArray(), timeField);
      if (fieldIndex < 0) return null;

      DateTime? minTime = null;
      DateTime? maxTime = null;

      for (int i = 0; i < reader.RowGroupCount; i++) {
        cancellationToken.ThrowIfCancellationRequested();
        using var rowGroupReader = reader.OpenRowGroupReader(i);

        var meta = rowGroupReader.RowGroup.Columns[fieldIndex].MetaData;
        if (meta != null && meta.Statistics != null) {
          var stats = meta.Statistics;

          if (stats.Min != null) {
            DateTime? dMin = DecodeDateTimeBytes(stats.Min);
            if (dMin != null && (minTime == null || dMin < minTime)) minTime = dMin;
          }

          if (stats.Max != null) {
            DateTime? dMax = DecodeDateTimeBytes(stats.Max);
            if (dMax != null && (maxTime == null || dMax > maxTime)) maxTime = dMax;
          }
        }
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
  /// Decodes raw Parquet Int64 timestamp bytes based on typical epoch heuristics.
  /// </summary>
  private static DateTime? DecodeDateTimeBytes(byte[] bytes)
  {
    if (bytes == null || bytes.Length < 8) return null;

    long val = BitConverter.ToInt64(bytes, 0);

    // Heuristic map to correctly decode the timestamp based on magnitude
    // Milliseconds in recent years are ~1.7e12
    // Microseconds in recent years are ~1.7e15
    // Ticks are ~6.3e17

    if (val > 100000000000000000L) {
      // Likely Ticks since 0001-01-01
      return new DateTime(val, DateTimeKind.Utc);
    } else if (val > 100000000000000L) {
      // Likely Microseconds since 1970-01-01
      return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(val * 10);
    } else {
      // Likely Milliseconds since 1970-01-01
      return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(val);
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
}
