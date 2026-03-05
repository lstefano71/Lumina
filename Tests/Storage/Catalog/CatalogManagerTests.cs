using Lumina.Storage.Catalog;

using Microsoft.Extensions.Logging;

using System.Collections.Concurrent;

using Xunit;

namespace Lumina.Tests.Storage.Catalog;

public sealed class CatalogManagerTests : IDisposable
{
  private readonly string _testDirectory;
  private readonly CatalogOptions _options;
  private readonly ILogger<CatalogManager> _logger;

  public CatalogManagerTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"catalog_test_{Guid.NewGuid():N}");
    Directory.CreateDirectory(_testDirectory);

    _options = new CatalogOptions {
      CatalogDirectory = _testDirectory,
      EnableAutoRebuild = false,
      EnableStartupGc = false
    };

    _logger = LoggerFactory.Create(builder => builder.AddDebug()).CreateLogger<CatalogManager>();
  }

  public void Dispose()
  {
    if (Directory.Exists(_testDirectory)) {
      Directory.Delete(_testDirectory, recursive: true);
    }
  }

  [Fact]
  public async Task InitializeAsync_ShouldCreateEmptyCatalog_WhenNoExistingCatalog()
  {
    var manager = new CatalogManager(_options, _logger);

    await manager.InitializeAsync();

    var streams = manager.GetStreams();
    Assert.Empty(streams);
  }

  [Fact]
  public async Task AddFileAsync_ShouldAddEntryAndPersist()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    var entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "test.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    };

    await manager.AddFileAsync(entry);

    var entries = manager.GetEntries();
    Assert.Single(entries);
    Assert.Equal("test-stream", entries[0].StreamName);
    Assert.Equal(StorageLevel.L1, entries[0].Level);
    Assert.Equal(100, entries[0].RowCount);
  }

  [Fact]
  public async Task AddFileAsync_ShouldNotDuplicateEntries()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    var entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "test.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    };

    await manager.AddFileAsync(entry);
    await manager.AddFileAsync(entry); // Add same file again

    var entries = manager.GetEntries();
    Assert.Single(entries);
  }

  [Fact]
  public async Task ReplaceFilesAsync_ShouldAtomicallySwapFiles()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    // Add initial L1 files
    var l1Entry1 = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-2),
      MaxTime = now.AddHours(-1),
      FilePath = Path.Combine(_testDirectory, "l1_1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 50,
      FileSizeBytes = 512,
      AddedAt = DateTime.UtcNow
    };

    var l1Entry2 = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "l1_2.parquet"),
      Level = StorageLevel.L1,
      RowCount = 50,
      FileSizeBytes = 512,
      AddedAt = DateTime.UtcNow
    };

    await manager.AddFileAsync(l1Entry1);
    await manager.AddFileAsync(l1Entry2);

    // Replace with L2 file
    var l2Entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-2),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "l2_daily.parquet"),
      Level = StorageLevel.L2,
      RowCount = 100,
      FileSizeBytes = 768,
      AddedAt = DateTime.UtcNow
    };

    await manager.ReplaceFilesAsync(
        new[] { l1Entry1.FilePath, l1Entry2.FilePath },
        l2Entry);

    var entries = manager.GetEntries();
    Assert.Single(entries);
    Assert.Equal(StorageLevel.L2, entries[0].Level);
    Assert.Equal(100, entries[0].RowCount);
  }

  [Fact]
  public async Task RemoveFileAsync_ShouldRemoveEntry()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    var entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "test.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    };

    await manager.AddFileAsync(entry);
    await manager.RemoveFileAsync(entry.FilePath);

    var entries = manager.GetEntries();
    Assert.Empty(entries);
  }

  [Fact]
  public async Task GetStreams_ShouldReturnUniqueStreamNames()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "a.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-b",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "b.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      MinTime = now.AddDays(-1).AddHours(-1),
      MaxTime = now.AddDays(-1),
      FilePath = Path.Combine(_testDirectory, "a2.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    var streams = manager.GetStreams();
    Assert.Equal(2, streams.Count);
    Assert.Contains("stream-a", streams);
    Assert.Contains("stream-b", streams);
  }

  [Fact]
  public async Task GetFiles_ShouldReturnFilesForStream()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "a1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      MinTime = now.AddHours(-2),
      MaxTime = now.AddHours(-1),
      FilePath = Path.Combine(_testDirectory, "a2.parquet"),
      Level = StorageLevel.L2,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-b",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "b.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    var files = manager.GetFiles("stream-a");
    Assert.Equal(2, files.Count);
  }

  [Fact]
  public async Task GetEntries_ShouldFilterByLevel()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "l1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      MinTime = now.AddHours(-2),
      MaxTime = now.AddHours(-1),
      FilePath = Path.Combine(_testDirectory, "l2.parquet"),
      Level = StorageLevel.L2,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    var l1Entries = manager.GetEntries(level: StorageLevel.L1);
    var l2Entries = manager.GetEntries(level: StorageLevel.L2);

    Assert.Single(l1Entries);
    Assert.Single(l2Entries);
    Assert.Equal(StorageLevel.L1, l1Entries[0].Level);
    Assert.Equal(StorageLevel.L2, l2Entries[0].Level);
  }

  [Fact]
  public async Task GetTotalSize_ShouldSumFileSizeBytes()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "a.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 1000,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      MinTime = now.AddHours(-2),
      MaxTime = now.AddHours(-1),
      FilePath = Path.Combine(_testDirectory, "b.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 2000,
      AddedAt = DateTime.UtcNow
    });

    var totalSize = manager.GetTotalSize();
    Assert.Equal(3000, totalSize);
  }

  [Fact]
  public async Task PersistAsync_ShouldUseSafeWritePattern()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    var entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "test.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    };

    await manager.AddFileAsync(entry);

    // Verify catalog.json exists
    var catalogPath = Path.Combine(_testDirectory, "catalog.json");
    Assert.True(File.Exists(catalogPath));

    // Verify temp file was cleaned up
    var tempPath = Path.Combine(_testDirectory, "catalog.tmp.json");
    Assert.False(File.Exists(tempPath));
  }

  [Fact]
  public async Task InitializeAsync_ShouldLoadExistingCatalog()
  {
    var now = DateTime.UtcNow;
    // Create initial catalog
    var manager1 = new CatalogManager(_options, _logger);
    await manager1.InitializeAsync();

    await manager1.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "test.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    });

    // Create new manager instance
    var manager2 = new CatalogManager(_options, _logger);
    await manager2.InitializeAsync();

    var entries = manager2.GetEntries();
    Assert.Single(entries);
    Assert.Equal("test-stream", entries[0].StreamName);
  }

  [Fact]
  public async Task ReloadFromStateAsync_ShouldReplaceEntireCatalog()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "old-stream",
      MinTime = now.AddHours(-1),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "old.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    });

    var newCatalog = new StreamCatalog {
      Entries = new List<CatalogEntry>
        {
                new()
                {
                    StreamName = "new-stream",
                    MinTime = now.AddHours(-1),
                    MaxTime = now,
                    FilePath = Path.Combine(_testDirectory, "new.parquet"),
                    Level = StorageLevel.L2,
                    RowCount = 200,
                    FileSizeBytes = 2048,
                    AddedAt = DateTime.UtcNow
                }
            },
      LastModified = DateTime.UtcNow,
      Version = 1
    };

    await manager.ReloadFromStateAsync(newCatalog);

    var entries = manager.GetEntries();
    Assert.Single(entries);
    Assert.Equal("new-stream", entries[0].StreamName);
    Assert.Equal(StorageLevel.L2, entries[0].Level);
  }

  [Fact]
  public async Task GetFilesInRange_ShouldReturnOverlappingFiles()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var baseTime = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);

    // File 1: 10:00 - 11:00
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(-2),
      MaxTime = baseTime.AddHours(-1),
      FilePath = Path.Combine(_testDirectory, "file1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    // File 2: 11:00 - 12:00
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(-1),
      MaxTime = baseTime,
      FilePath = Path.Combine(_testDirectory, "file2.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    // File 3: 12:00 - 13:00
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime,
      MaxTime = baseTime.AddHours(1),
      FilePath = Path.Combine(_testDirectory, "file3.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    // Query: 11:30 - 12:30 (should match File 2 and File 3)
    var entries = manager.GetFilesInRange("test-stream", baseTime.AddMinutes(-30), baseTime.AddMinutes(30));
    Assert.Equal(2, entries.Count);
  }

  [Fact]
  public async Task GetEligibleForDailyCompaction_ShouldReturnL1FilesBeforeCutoff()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    var now = DateTime.UtcNow;
    var yesterday = now.AddDays(-1);

    // L1 file from yesterday (eligible)
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = yesterday.AddHours(-2),
      MaxTime = yesterday,
      FilePath = Path.Combine(_testDirectory, "eligible.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    // L1 file from today (not eligible)
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = now.AddHours(-2),
      MaxTime = now,
      FilePath = Path.Combine(_testDirectory, "not_eligible.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    // L2 file from yesterday (not eligible - wrong level)
    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      MinTime = yesterday.AddHours(-2),
      MaxTime = yesterday,
      FilePath = Path.Combine(_testDirectory, "l2_file.parquet"),
      Level = StorageLevel.L2,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    var eligible = manager.GetEligibleForDailyCompaction("test-stream", now.Date);
    Assert.Single(eligible);
    Assert.Equal("eligible.parquet", Path.GetFileName(eligible[0].FilePath));
  }

  [Fact]
  public async Task ConcurrentReadsDuringReplaceFiles_ShouldObserveCoherentSnapshots()
  {
    var manager = new CatalogManager(_options, _logger);
    await manager.InitializeAsync();

    const string stream = "race-stream";
    const int fixedFileCount = 20;
    var now = DateTime.UtcNow;
    var currentPaths = new List<string>(capacity: fixedFileCount);
    var currentPathsLock = new object();

    for (int i = 0; i < fixedFileCount; i++) {
      var path = Path.Combine(_testDirectory, $"seed_{i:D3}.parquet");
      currentPaths.Add(path);

      await manager.AddFileAsync(new CatalogEntry {
        StreamName = stream,
        MinTime = now.AddMinutes(-i - 1),
        MaxTime = now.AddMinutes(-i),
        FilePath = path,
        Level = StorageLevel.L1,
        RowCount = 10,
        FileSizeBytes = 100,
        AddedAt = DateTime.UtcNow
      });
    }

    var failures = new ConcurrentQueue<string>();
    var writerDone = false;

    var writer = Task.Run(async () => {
      for (int i = 0; i < 500; i++) {
        var slot = i % fixedFileCount;
        string oldPath;
        lock (currentPathsLock) {
          oldPath = currentPaths[slot];
        }

        var newPath = Path.Combine(_testDirectory, $"swap_{i:D4}.parquet");
        await manager.ReplaceFilesAsync(
            new[] { oldPath },
            new CatalogEntry {
              StreamName = stream,
              MinTime = now.AddMinutes(i),
              MaxTime = now.AddMinutes(i + 1),
              FilePath = newPath,
              Level = StorageLevel.L1,
              RowCount = 10,
              FileSizeBytes = 100,
              AddedAt = DateTime.UtcNow
            });

        lock (currentPathsLock) {
          currentPaths[slot] = newPath;
        }
      }

      writerDone = true;
    });

    var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () => {
      while (!writerDone) {
        try {
          var files = manager.GetFiles(stream);
          var entries = manager.GetEntries(stream: stream);
          var streams = manager.GetStreams();
          var totalSize = manager.GetTotalSize();

          if (files.Count != fixedFileCount) {
            failures.Enqueue($"Expected {fixedFileCount} files, got {files.Count}");
          }

          if (entries.Count != fixedFileCount) {
            failures.Enqueue($"Expected {fixedFileCount} entries, got {entries.Count}");
          }

          if (!streams.Contains(stream)) {
            failures.Enqueue("Expected stream to remain visible in GetStreams");
          }

          if (totalSize <= 0) {
            failures.Enqueue("Expected total size to remain positive");
          }

          var distinctFiles = files.Distinct(StringComparer.OrdinalIgnoreCase).Count();
          if (distinctFiles != files.Count) {
            failures.Enqueue("Detected duplicate file paths in a read snapshot");
          }
        } catch (Exception ex) {
          failures.Enqueue($"Reader exception: {ex.GetType().Name}: {ex.Message}");
        }

        await Task.Yield();
      }
    })).ToArray();

    await writer;
    await Task.WhenAll(readers);

    Assert.True(failures.IsEmpty, string.Join(Environment.NewLine, failures.Take(10)));

    var finalFiles = manager.GetFiles(stream);
    Assert.Equal(fixedFileCount, finalFiles.Count);
  }
}