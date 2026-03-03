using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Compaction;

using System.Text;
using System.Text.Json;

using Xunit;

namespace Lumina.Tests.Storage;

public class CursorValidatorTests : WalTestBase
{
  private readonly CursorValidator _validator = new();
  private string CursorDir => Path.Combine(TempDirectory, "cursors");

  [Fact]
  public async Task ValidateAsync_NotFound_ShouldReturnNotFound()
  {
    var result = await _validator.ValidateAsync(Path.Combine(CursorDir, "nonexistent.cursor"));

    result.Result.Should().Be(CursorValidationResult.NotFound);
    result.Cursor.Should().BeNull();
  }

  [Fact]
  public async Task ValidateAsync_ValidCursor_ShouldReturnValid()
  {
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 12345,
      LastParquetFile = "/data/test.parquet",
      LastCompactionTime = DateTime.UtcNow
    };

    var filePath = CreateValidCursorFile(cursor);

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.Valid);
    result.Cursor.Should().NotBeNull();
    result.Cursor!.Stream.Should().Be("test-stream");
    result.Cursor.LastCompactedOffset.Should().Be(12345);
  }

  [Fact]
  public async Task ValidateAsync_InvalidMagic_ShouldReturnInvalidMagic()
  {
    var filePath = Path.Combine(CursorDir, "bad.cursor");
    Directory.CreateDirectory(CursorDir);

    // Write invalid magic
    var badData = new byte[CursorFileHeader.Size + 10];
    BitConverter.TryWriteBytes(badData.AsSpan(0, 4), 0xDEADBEEF); // Wrong magic
    badData[4] = 1; // Version
    File.WriteAllBytes(filePath, badData);

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.InvalidMagic);
  }

  [Fact]
  public async Task ValidateAsync_UnsupportedVersion_ShouldReturnUnsupportedVersion()
  {
    var filePath = Path.Combine(CursorDir, "badversion.cursor");
    Directory.CreateDirectory(CursorDir);

    // Write valid magic but wrong version
    var badData = new byte[CursorFileHeader.Size + 10];
    BitConverter.TryWriteBytes(badData.AsSpan(0, 4), CursorFileHeader.ExpectedMagic);
    badData[4] = 0xFF; // Wrong version
    File.WriteAllBytes(filePath, badData);

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.UnsupportedVersion);
  }

  [Fact]
  public async Task ValidateAsync_TruncatedFile_ShouldReturnTruncatedPayload()
  {
    var filePath = Path.Combine(CursorDir, "truncated.cursor");
    Directory.CreateDirectory(CursorDir);

    // Write only header bytes
    var header = new CursorFileHeader(1234, 100); // Claims 100 bytes payload
    Span<byte> headerBytes = stackalloc byte[CursorFileHeader.Size];
    header.WriteTo(headerBytes);
    File.WriteAllBytes(filePath, headerBytes.ToArray());

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.TruncatedPayload);
  }

  [Fact]
  public async Task ValidateAsync_ChecksumMismatch_ShouldReturnChecksumMismatch()
  {
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 12345
    };

    var filePath = Path.Combine(CursorDir, "badchecksum.cursor");
    Directory.CreateDirectory(CursorDir);

    var json = JsonSerializer.Serialize(cursor);
    var payload = Encoding.UTF8.GetBytes(json);

    // Create header with wrong checksum
    var header = new CursorFileHeader(0xDEADBEEF, (uint)payload.Length); // Wrong checksum
    var fileBytes = new byte[CursorFileHeader.Size + payload.Length];
    header.WriteTo(fileBytes);
    Array.Copy(payload, 0, fileBytes, CursorFileHeader.Size, payload.Length);

    File.WriteAllBytes(filePath, fileBytes);

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.ChecksumMismatch);
  }

  [Fact]
  public async Task ValidateAsync_InvalidJson_ShouldReturnInvalidJson()
  {
    var filePath = Path.Combine(CursorDir, "invalidjson.cursor");
    Directory.CreateDirectory(CursorDir);

    var invalidJson = Encoding.UTF8.GetBytes("NOT VALID JSON {{{");
    var header = CursorFileHeader.CreateForPayload(invalidJson);
    var fileBytes = new byte[CursorFileHeader.Size + invalidJson.Length];
    header.WriteTo(fileBytes);
    Array.Copy(invalidJson, 0, fileBytes, CursorFileHeader.Size, invalidJson.Length);

    File.WriteAllBytes(filePath, fileBytes);

    var result = await _validator.ValidateAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.InvalidJson);
  }

  [Fact]
  public async Task ValidateWithCrossCheckAsync_WalFileNotFound_ShouldReturnWalFileNotFound()
  {
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedWalFile = "/nonexistent/path/file.wal",
      LastCompactedOffset = 12345
    };

    var filePath = CreateValidCursorFile(cursor);

    var result = await _validator.ValidateWithCrossCheckAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.WalFileNotFound);
  }

  [Fact]
  public async Task ValidateWithCrossCheckAsync_ParquetFileNotFound_ShouldReturnParquetFileNotFound()
  {
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 12345,
      LastParquetFile = "/nonexistent/path/file.parquet"
    };

    var filePath = CreateValidCursorFile(cursor);

    var result = await _validator.ValidateWithCrossCheckAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.ParquetFileNotFound);
  }

  [Fact]
  public async Task ValidateWithCrossCheckAsync_AllFilesExist_ShouldReturnValid()
  {
    // Create dummy files
    var walPath = Path.Combine(TempDirectory, "test.wal");
    var parquetPath = Path.Combine(TempDirectory, "test.parquet");
    await File.WriteAllTextAsync(walPath, "wal content");
    await File.WriteAllTextAsync(parquetPath, "parquet content");

    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedWalFile = walPath,
      LastCompactedOffset = 12345,
      LastParquetFile = parquetPath
    };

    var filePath = CreateValidCursorFile(cursor);

    var result = await _validator.ValidateWithCrossCheckAsync(filePath);

    result.Result.Should().Be(CursorValidationResult.Valid);
  }

  [Fact]
  public void CanRecover_NotFound_ShouldReturnTrue()
  {
    CursorValidator.CanRecover(CursorValidationResult.NotFound).Should().BeTrue();
  }

  [Fact]
  public void CanRecover_Valid_ShouldReturnFalse()
  {
    CursorValidator.CanRecover(CursorValidationResult.Valid).Should().BeFalse();
  }

  [Fact]
  public void CanRecover_CorruptionResults_ShouldReturnTrue()
  {
    CursorValidator.CanRecover(CursorValidationResult.ChecksumMismatch).Should().BeTrue();
    CursorValidator.CanRecover(CursorValidationResult.TruncatedPayload).Should().BeTrue();
    CursorValidator.CanRecover(CursorValidationResult.InvalidJson).Should().BeTrue();
    CursorValidator.CanRecover(CursorValidationResult.InvalidMagic).Should().BeTrue();
  }

  [Fact]
  public void IsCorruption_CorruptionResults_ShouldReturnTrue()
  {
    CursorValidator.IsCorruption(CursorValidationResult.ChecksumMismatch).Should().BeTrue();
    CursorValidator.IsCorruption(CursorValidationResult.TruncatedPayload).Should().BeTrue();
    CursorValidator.IsCorruption(CursorValidationResult.InvalidJson).Should().BeTrue();
    CursorValidator.IsCorruption(CursorValidationResult.InvalidMagic).Should().BeTrue();
  }

  [Fact]
  public void IsCorruption_NonCorruptionResults_ShouldReturnFalse()
  {
    CursorValidator.IsCorruption(CursorValidationResult.Valid).Should().BeFalse();
    CursorValidator.IsCorruption(CursorValidationResult.NotFound).Should().BeFalse();
    CursorValidator.IsCorruption(CursorValidationResult.WalFileNotFound).Should().BeFalse();
  }

  private string CreateValidCursorFile(CompactionCursor cursor)
  {
    Directory.CreateDirectory(CursorDir);
    var filePath = Path.Combine(CursorDir, $"{cursor.Stream}.cursor");

    var json = JsonSerializer.Serialize(cursor);
    var payload = Encoding.UTF8.GetBytes(json);
    var header = CursorFileHeader.CreateForPayload(payload);

    var fileBytes = new byte[CursorFileHeader.Size + payload.Length];
    header.WriteTo(fileBytes);
    Array.Copy(payload, 0, fileBytes, CursorFileHeader.Size, payload.Length);

    File.WriteAllBytes(filePath, fileBytes);
    return filePath;
  }
}