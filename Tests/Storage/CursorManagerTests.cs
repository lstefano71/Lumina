using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Compaction;

using Xunit;

namespace Lumina.Tests.Storage;

public class CursorManagerTests : WalTestBase
{
  private string CursorDir => Path.Combine(TempDirectory, "cursors");

  [Fact]
  public void GetCursor_NewStream_ShouldReturnDefaultCursor()
  {
    var manager = new CursorManager(CursorDir);

    var cursor = manager.GetCursor("new-stream");

    cursor.Should().NotBeNull();
    cursor.Stream.Should().Be("new-stream");
    cursor.LastCompactedOffset.Should().Be(0);
    cursor.LastParquetFile.Should().BeNull();
  }

  [Fact]
  public void UpdateCursor_ShouldPersistToFile()
  {
    var manager = new CursorManager(CursorDir);
    var cursor = new CompactionCursor {
      Stream = "test-stream",
      LastCompactedOffset = 12345,
      LastParquetFile = "/data/test.parquet",
      LastCompactionTime = DateTime.UtcNow
    };

    manager.UpdateCursor(cursor);

    var filePath = Path.Combine(CursorDir, "test-stream.cursor.json");
    File.Exists(filePath).Should().BeTrue();
  }

  [Fact]
  public void UpdateCursor_ShouldBeRetrievable()
  {
    var manager = new CursorManager(CursorDir);
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
    var manager1 = new CursorManager(CursorDir);
    manager1.UpdateCursor(new CompactionCursor {
      Stream = "persist-stream",
      LastCompactedOffset = 42000,
      LastParquetFile = "persist.parquet",
      LastCompactionTime = DateTime.UtcNow
    });

    // Create a new manager against the same directory
    var manager2 = new CursorManager(CursorDir);
    var cursor = manager2.GetCursor("persist-stream");

    cursor.LastCompactedOffset.Should().Be(42000);
    cursor.LastParquetFile.Should().Be("persist.parquet");
  }

  [Fact]
  public void MarkCompactionComplete_ShouldUpdateOffset()
  {
    var manager = new CursorManager(CursorDir);

    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file1.parquet");
    var cursor = manager.GetCursor("stream-a");

    cursor.LastCompactedOffset.Should().Be(5000);
    cursor.LastParquetFile.Should().Be("file1.parquet");
  }

  [Fact]
  public void MarkCompactionComplete_ShouldNotGoBackwards()
  {
    var manager = new CursorManager(CursorDir);

    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file1.parquet");
    manager.MarkCompactionComplete("stream-a", "file1.wal", 3000, "file0.parquet"); // earlier offset

    var cursor = manager.GetCursor("stream-a");
    cursor.LastCompactedOffset.Should().Be(5000, "cursor should not move backwards");
  }

  [Fact]
  public void IsOffsetCompacted_TrueForOlderOffset()
  {
    var manager = new CursorManager(CursorDir);
    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file.parquet");

    manager.IsOffsetCompacted("stream-a", "file1.wal", 4000).Should().BeTrue();
    manager.IsOffsetCompacted("stream-a", "file1.wal", 5000).Should().BeTrue();
  }

  [Fact]
  public void IsOffsetCompacted_FalseForNewerOffset()
  {
    var manager = new CursorManager(CursorDir);
    manager.MarkCompactionComplete("stream-a", "file1.wal", 5000, "file.parquet");

    manager.IsOffsetCompacted("stream-a", "file1.wal", 6000).Should().BeFalse();
  }

  [Fact]
  public void GetAllCursors_ShouldReturnAllUpdatedCursors()
  {
    var manager = new CursorManager(CursorDir);

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

    var _ = new CursorManager(newDir);

    Directory.Exists(newDir).Should().BeTrue();
  }

  [Fact]
  public void CursorManager_ShouldIgnoreCorruptedCursorFiles()
  {
    // Pre-create a corrupt cursor file
    Directory.CreateDirectory(CursorDir);
    File.WriteAllText(Path.Combine(CursorDir, "bad.cursor.json"), "NOT VALID JSON!!!");

    var act = () => new CursorManager(CursorDir);

    act.Should().NotThrow("corrupted cursor files should be silently skipped");
  }

  [Fact]
  public void UpdateCursor_AtomicWrite_TempFileShouldNotLinger()
  {
    var manager = new CursorManager(CursorDir);
    manager.UpdateCursor(new CompactionCursor {
      Stream = "atomic-test",
      LastCompactedOffset = 100
    });

    var tmpFiles = Directory.GetFiles(CursorDir, "*.tmp");
    tmpFiles.Should().BeEmpty("atomic writes should clean up temp files");
  }
}
