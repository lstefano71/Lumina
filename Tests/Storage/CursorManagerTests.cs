using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Compaction;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Storage;

public class CursorManagerTests : WalTestBase
{
  private string CursorDir => Path.Combine(TempDirectory, "cursors");
  private readonly CursorValidator _validator = new();

  private CursorManager CreateManager(
      bool enableValidation = true,
      bool enableRecovery = true,
      CursorRecoveryService? recoveryService = null)
  {
    return new CursorManager(
        CursorDir,
        _validator,
        recoveryService,
        NullLogger<CursorManager>.Instance,
        enableValidation,
        enableRecovery);
  }

  [Fact]
  public void GetCursor_NewStream_ShouldReturnDefaultCursor()
  {
    var manager = CreateManager();

    var cursor = manager.GetCursor("new-stream");

    cursor.Should().NotBeNull();
    cursor.Stream.Should().Be("new-stream");
    cursor.LastCompactedOffset.Should().Be(0);
    cursor.LastParquetFile.Should().BeNull();
  }

  [Fact]
  public void UpdateCursor_ShouldPersistToFile()
  {
    var manager = CreateManager();
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 12345,
      LastParquetFile = "/data/test.parquet",
      LastCompactionTime = DateTime.UtcNow
    };

    manager.UpdateCursor(cursor);

    var filePath = Path.Combine(CursorDir, "test-stream.cursor");
    File.Exists(filePath).Should().BeTrue();
  }

  [Fact]
  public void UpdateCursor_ShouldBeRetrievable()
  {
    var manager = CreateManager();
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 9999,
      LastParquetFile = "output.parquet",
      LastCompactionTime = DateTime.UtcNow
    };

    manager.UpdateCursor(cursor);
    var retrieved = manager.GetCursor("test-stream");

    retrieved.LastCompactedOffset.Should().Be(9999);
    retrieved.LastParquetFile.Should().Be("output.parquet");
  }

  [Fact]
  public void CursorManager_ShouldSurviveRestart()
  {
    // Write a cursor with one manager
    var manager1 = CreateManager();
    manager1.UpdateCursor(new CompactionCursor {
      Stream = "persist-stream",
      LastCompactedOffset = 42000,
      LastParquetFile = "persist.parquet",
      LastCompactionTime = DateTime.UtcNow
    });

    // Create a new manager against the same directory
    var manager2 = CreateManager();
    var cursor = manager2.GetCursor("persist-stream");

    cursor.LastCompactedOffset.Should().Be(42000);
    cursor.LastParquetFile.Should().Be("persist.parquet");
  }

  [Fact]
  public void MarkCompactionComplete_ShouldUpdateOffset()
  {
    var manager = CreateManager();

    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file1.parquet");
    var cursor = manager.GetCursor("stream-a");

    cursor.LastCompactedOffset.Should().Be(5000);
    cursor.LastParquetFile.Should().Be("file1.parquet");
  }

  [Fact]
  public void MarkCompactionComplete_ShouldNotGoBackwards()
  {
    var manager = CreateManager();

    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file1.parquet");
    manager.MarkCompactionComplete("stream-a", "file1.wal", 3000, "file0.parquet"); // earlier offset

    var cursor = manager.GetCursor("stream-a");
    cursor.LastCompactedOffset.Should().Be(5000, "cursor should not move backwards");
  }

  [Fact]
  public void IsOffsetCompacted_TrueForOlderOffset()
  {
    var manager = CreateManager();
    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file.parquet");

    manager.IsOffsetCompacted("stream-a", "file1.wal", 4000).Should().BeTrue();
    manager.IsOffsetCompacted("stream-a", "file1.wal", 5000).Should().BeTrue();
  }

  [Fact]
  public void IsOffsetCompacted_FalseForNewerOffset()
  {
    var manager = CreateManager();
    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file.parquet");

    manager.IsOffsetCompacted("stream-a", "file1.wal", 6000).Should().BeFalse();
  }

  [Fact]
  public void GetAllCursors_ShouldReturnAllUpdatedCursors()
  {
    var manager = CreateManager();

    manager.MarkCompactionComplete("alpha", "a.wal", 100, "a.parquet");
    manager.MarkCompactionComplete("beta", "b.wal", 200, "b.parquet");
    manager.MarkCompactionComplete("gamma", "c.wal", 300, "c.parquet");

    var all = manager.GetAllCursors();

    all.Should().HaveCount(3);
    all.Select(c => c.Stream).Should().BeEquivalentTo(new[] { "alpha", "beta", "gamma" });
  }

  [Fact]
  public void Constructor_ShouldCreateDirectoryIfNotExists()
  {
    var newDir = Path.Combine(TempDirectory, "brand-new-cursor-dir");
    Directory.Exists(newDir).Should().BeFalse();

    var _ = new CursorManager(
        newDir,
        _validator,
        null,
        NullLogger<CursorManager>.Instance,
        enableValidation: true,
        enableRecovery: false);

    Directory.Exists(newDir).Should().BeTrue();
  }

  [Fact]
  public void CursorManager_ShouldHandleCorruptedCursorFiles()
  {
    // Pre-create a corrupt cursor file
    Directory.CreateDirectory(CursorDir);
    File.WriteAllText(Path.Combine(CursorDir, "bad.cursor"), "NOT VALID DATA!!!");

    var act = () => CreateManager();

    act.Should().NotThrow("corrupted cursor files should be handled gracefully");
  }

  [Fact]
  public void UpdateCursor_AtomicWrite_TempFileShouldNotLinger()
  {
    var manager = CreateManager();
    manager.UpdateCursor(new CompactionCursor {
      Stream = "atomic-test",
      LastCompactedOffset = 100
    });

    var tmpFiles = Directory.GetFiles(CursorDir, "*.tmp");
    tmpFiles.Should().BeEmpty("atomic writes should clean up temp files");
  }

  [Fact]
  public void MarkCompactionComplete_WithValidationFields_ShouldPersistFields()
  {
    var manager = CreateManager();

    manager.MarkCompactionComplete(
        "test-stream",
        "file.wal",
        5000,
        "file.parquet",
        walFileSize: 123456,
        parquetEntryCount: 100);

    var cursor = manager.GetCursor("test-stream");

    cursor.LastWalFileSize.Should().Be(123456);
    cursor.LastParquetEntryCount.Should().Be(100);
  }

  [Fact]
  public void CursorFile_ShouldHaveHeaderWithChecksum()
  {
    var manager = CreateManager();
    manager.UpdateCursor(new CompactionCursor {
      Stream = "checksum-test",
      LastCompactedOffset = 999
    });

    var filePath = Path.Combine(CursorDir, "checksum-test.cursor");
    var bytes = File.ReadAllBytes(filePath);

    bytes.Length.Should().BeGreaterOrEqualTo(CursorFileHeader.Size);

    var header = CursorFileHeader.ReadFrom(bytes);
    header.HasValidMagic.Should().BeTrue();
    header.HasSupportedVersion.Should().BeTrue();
    header.PayloadLength.Should().BeGreaterThan(0);
  }

  [Fact]
  public void CursorManager_ShouldMigrateOldJsonFiles()
  {
    // Create old-style JSON cursor file
    Directory.CreateDirectory(CursorDir);
    var oldCursor = new CompactionCursor {
      Stream = "legacy-stream",
      LastCompactedOffset = 12345
    };
    var json = System.Text.Json.JsonSerializer.Serialize(oldCursor);
    File.WriteAllText(Path.Combine(CursorDir, "legacy-stream.cursor.json"), json);

    var manager = CreateManager();

    // Old file should be migrated
    var cursor = manager.GetCursor("legacy-stream");
    cursor.LastCompactedOffset.Should().Be(12345);

    // Old file should be removed
    File.Exists(Path.Combine(CursorDir, "legacy-stream.cursor.json")).Should().BeFalse();

    // New file should exist
    File.Exists(Path.Combine(CursorDir, "legacy-stream.cursor")).Should().BeTrue();
  }

  [Fact]
  public void GetRecoveryStats_ShouldReturnEmptyInitially()
  {
    var manager = CreateManager();

    var stats = manager.GetRecoveryStats();

    stats.Should().BeEmpty();
  }

  [Fact]
  public void MarkCompactionComplete_WithNewerFile_ShouldUpdate()
  {
    var manager = CreateManager();

    manager.MarkCompactionComplete("stream", "file1.wal", 100, "file1.parquet");
    manager.MarkCompactionComplete("stream", "file2.wal", 50, "file2.parquet");

    var cursor = manager.GetCursor("stream");
    cursor.LastCompactedWalFile.Should().Be("file2.wal");
    cursor.LastCompactedOffset.Should().Be(50);
  }
}