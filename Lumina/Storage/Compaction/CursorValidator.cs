using Lumina.Core.Models;

using System.Text.Json;

namespace Lumina.Storage.Compaction;

/// <summary>
/// Validates cursor file integrity and cross-references with the filesystem.
/// </summary>
public sealed class CursorValidator
{
  private static readonly JsonSerializerOptions JsonOptions = new() {
    PropertyNameCaseInsensitive = true
  };

  /// <summary>
  /// Validates a cursor file and returns the parsed cursor if valid.
  /// </summary>
  /// <param name="filePath">The path to the cursor file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A tuple containing the validation result and the parsed cursor (if valid).</returns>
  public async Task<(CursorValidationResult Result, CompactionCursor? Cursor)> ValidateAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    if (!File.Exists(filePath)) {
      return (CursorValidationResult.NotFound, null);
    }

    byte[] fileBytes;
    try {
      fileBytes = await File.ReadAllBytesAsync(filePath, cancellationToken);
    } catch (Exception) {
      return (CursorValidationResult.TruncatedPayload, null);
    }

    // Check minimum file size (header only)
    if (fileBytes.Length < CursorFileHeader.Size) {
      return (CursorValidationResult.TruncatedPayload, null);
    }

    // Parse and validate header
    var header = CursorFileHeader.ReadFrom(fileBytes);

    if (!header.HasValidMagic) {
      return (CursorValidationResult.InvalidMagic, null);
    }

    if (!header.HasSupportedVersion) {
      return (CursorValidationResult.UnsupportedVersion, null);
    }

    // Check payload length
    var expectedLength = CursorFileHeader.Size + header.PayloadLength;
    if (fileBytes.Length < expectedLength) {
      return (CursorValidationResult.TruncatedPayload, null);
    }

    // Extract and validate payload
    var payload = fileBytes.AsSpan(CursorFileHeader.Size, (int)header.PayloadLength);

    if (!header.ValidatePayload(payload)) {
      return (CursorValidationResult.ChecksumMismatch, null);
    }

    // Deserialize JSON payload
    CompactionCursor? cursor;
    try {
      cursor = JsonSerializer.Deserialize<CompactionCursor>(payload, JsonOptions);
    } catch (JsonException) {
      return (CursorValidationResult.InvalidJson, null);
    }

    if (cursor == null) {
      return (CursorValidationResult.InvalidJson, null);
    }

    return (CursorValidationResult.Valid, cursor);
  }

  /// <summary>
  /// Validates the header of a cursor file without full deserialization.
  /// </summary>
  /// <param name="filePath">The path to the cursor file.</param>
  /// <returns>A tuple containing the validation result and the header if readable.</returns>
  public (CursorValidationResult Result, CursorFileHeader Header) ValidateHeader(string filePath)
  {
    if (!File.Exists(filePath)) {
      return (CursorValidationResult.NotFound, default);
    }

    Span<byte> headerBytes = stackalloc byte[CursorFileHeader.Size];
    try {
      using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
      int read = fs.Read(headerBytes);
      if (read < CursorFileHeader.Size) {
        return (CursorValidationResult.TruncatedPayload, default);
      }
    } catch (Exception) {
      return (CursorValidationResult.TruncatedPayload, default);
    }

    if (!CursorFileHeader.TryRead(headerBytes, out var header)) {
      return (CursorValidationResult.TruncatedPayload, default);
    }

    if (!header.HasValidMagic) {
      return (CursorValidationResult.InvalidMagic, header);
    }

    if (!header.HasSupportedVersion) {
      return (CursorValidationResult.UnsupportedVersion, header);
    }

    return (CursorValidationResult.Valid, header);
  }

  /// <summary>
  /// Cross-validates that the files referenced by the cursor still exist.
  /// </summary>
  /// <param name="cursor">The cursor to validate.</param>
  /// <returns>The validation result.</returns>
  public CursorValidationResult CrossValidateFiles(CompactionCursor cursor)
  {
    // Check WAL file existence
    if (!string.IsNullOrEmpty(cursor.LastCompactedWalFile)) {
      if (!File.Exists(cursor.LastCompactedWalFile)) {
        return CursorValidationResult.WalFileNotFound;
      }
    }

    // Check Parquet file existence
    if (!string.IsNullOrEmpty(cursor.LastParquetFile)) {
      if (!File.Exists(cursor.LastParquetFile)) {
        return CursorValidationResult.ParquetFileNotFound;
      }
    }

    return CursorValidationResult.Valid;
  }

  /// <summary>
  /// Validates a cursor file and cross-references with the filesystem.
  /// </summary>
  /// <param name="filePath">The path to the cursor file.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>A tuple containing the validation result and the parsed cursor (if valid).</returns>
  public async Task<(CursorValidationResult Result, CompactionCursor? Cursor)> ValidateWithCrossCheckAsync(
      string filePath,
      CancellationToken cancellationToken = default)
  {
    var (result, cursor) = await ValidateAsync(filePath, cancellationToken);

    if (result != CursorValidationResult.Valid || cursor == null) {
      return (result, cursor);
    }

    // Perform cross-validation
    var crossCheckResult = CrossValidateFiles(cursor);
    return (crossCheckResult, cursor);
  }

  /// <summary>
  /// Determines if a validation result can be recovered from.
  /// </summary>
  /// <param name="result">The validation result.</param>
  /// <returns>True if recovery is possible.</returns>
  public static bool CanRecover(CursorValidationResult result)
  {
    return result switch {
      CursorValidationResult.Valid => false,
      CursorValidationResult.NotFound => true, // Create new cursor
      CursorValidationResult.InvalidMagic => true, // Rebuild from WAL/Parquet
      CursorValidationResult.UnsupportedVersion => true, // Rebuild from WAL/Parquet
      CursorValidationResult.ChecksumMismatch => true, // Rebuild from WAL/Parquet
      CursorValidationResult.TruncatedPayload => true, // Rebuild from WAL/Parquet
      CursorValidationResult.InvalidJson => true, // Rebuild from WAL/Parquet
      CursorValidationResult.WalFileNotFound => true, // Adjust cursor to existing files
      CursorValidationResult.ParquetFileNotFound => true, // Adjust cursor to existing files
      _ => false
    };
  }

  /// <summary>
  /// Determines if a validation result indicates corruption.
  /// </summary>
  /// <param name="result">The validation result.</param>
  /// <returns>True if the result indicates corruption.</returns>
  public static bool IsCorruption(CursorValidationResult result)
  {
    return result switch {
      CursorValidationResult.InvalidMagic => true,
      CursorValidationResult.ChecksumMismatch => true,
      CursorValidationResult.TruncatedPayload => true,
      CursorValidationResult.InvalidJson => true,
      _ => false
    };
  }
}