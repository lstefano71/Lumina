using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Compaction;
using Lumina.Storage.Wal;

using Microsoft.Extensions.Logging.Abstractions;

using System.Text;
using System.Text.Json;

using Xunit;

namespace Lumina.Tests.Storage;

public class CursorRecoveryTests : WalTestBase
{
  private readonly CursorValidator _validator = new();
  private string CursorDir => Path.Combine(TempDirectory, "cursors");

  private CompactionSettings CreateCompactionSettings()
  {
    return new CompactionSettings {
      L1Directory = Path.Combine(TempDirectory, "l1"),
      CursorDirectory = CursorDir
    };
  }

  private CursorRecoveryService CreateRecoveryService(WalManager walManager, CompactionSettings settings)
  {
    return new CursorRecoveryService(walManager, settings, NullLogger<CursorRecoveryService>.Instance);
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

  [Fact]
  public async Task TryRecoverAsync_NotFound_ShouldCreateNewCursor()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var (success, cursor, info) = await recoveryService.TryRecoverAsync(
        "new-stream",
        CursorValidationResult.NotFound,
        null);

    success.Should().BeTrue();
    cursor.Should().NotBeNull();
    cursor!.Stream.Should().Be("new-stream");
    cursor.LastCompactedOffset.Should().Be(0);
    info.RecoveryMethod.Should().Be("NewCursor");
  }

  [Fact]
  public async Task TryRecoverAsync_ChecksumMismatch_ShouldRecover()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    // Create a cursor and then simulate checksum mismatch
    var originalCursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 5000
    };

    var (success, cursor, info) = await recoveryService.TryRecoverAsync(
        "test-stream",
        CursorValidationResult.ChecksumMismatch,
        originalCursor);

    success.Should().BeTrue();
    cursor.Should().NotBeNull();
    info.WasRecovered.Should().BeTrue();
  }

  [Fact]
  public async Task RebuildFromWalFilesAsync_NoWalFiles_ShouldReturnNull()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var cursor = await recoveryService.RebuildFromWalFilesAsync("nonexistent-stream");

    cursor.Should().BeNull();
  }

  [Fact]
  public async Task RebuildFromWalFilesAsync_WithWalFiles_ShouldReturnCursor()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    // Write some entries to WAL
    const string stream = "recovery-stream";
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"message-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(entries);

    // Force rotate to seal the WAL file
    await walManager.ForceRotateAsync(stream);

    var cursor = await recoveryService.RebuildFromWalFilesAsync(stream);

    cursor.Should().NotBeNull();
    cursor!.Stream.Should().Be(stream);
    cursor.LastCompactedWalFile.Should().NotBeNull();
  }

  [Fact]
  public async Task RebuildFromParquetFilesAsync_NoParquetFiles_ShouldReturnNull()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var cursor = await recoveryService.RebuildFromParquetFilesAsync("nonexistent-stream");

    cursor.Should().BeNull();
  }

  [Fact]
  public async Task TryRecoverAsync_WalFileNotFound_ShouldAdjustCursor()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var originalCursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedWalFile = "/nonexistent/file.wal",
      LastCompactedOffset = 5000
    };

    var (success, cursor, info) = await recoveryService.TryRecoverAsync(
        "test-stream",
        CursorValidationResult.WalFileNotFound,
        originalCursor);

    success.Should().BeTrue();
    info.WasRecovered.Should().BeTrue();
  }

  [Fact]
  public async Task TryRecoverAsync_ParquetFileNotFound_ShouldAdjustCursor()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var originalCursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 5000,
      LastParquetFile = "/nonexistent/file.parquet"
    };

    var (success, cursor, info) = await recoveryService.TryRecoverAsync(
        "test-stream",
        CursorValidationResult.ParquetFileNotFound,
        originalCursor);

    success.Should().BeTrue();
    info.WasRecovered.Should().BeTrue();
  }

  [Fact]
  public async Task ValidateAndRecoverAllAsync_NoIssues_ShouldReturnEmpty()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var results = await recoveryService.ValidateAndRecoverAllAsync();

    results.Should().BeEmpty();
  }

  [Fact]
  public async Task RecoveryInfo_ShouldContainCorrectData()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var recoveryService = CreateRecoveryService(walManager, compactionSettings);

    var originalCursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 9999
    };

    var (_, _, info) = await recoveryService.TryRecoverAsync(
        "test-stream",
        CursorValidationResult.ChecksumMismatch,
        originalCursor);

    info.Stream.Should().Be("test-stream");
    info.ValidationResult.Should().Be(CursorValidationResult.ChecksumMismatch);
    info.OriginalOffset.Should().Be(9999);
    info.RecoveryTimestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(5));
  }
}