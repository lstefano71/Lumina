using Lumina.Storage.Catalog;

using Microsoft.Extensions.Logging;

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

    var entry = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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

    var entry = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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

    // Add initial L1 files
    var l1Entry1 = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "l1_1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 50,
      FileSizeBytes = 512,
      AddedAt = DateTime.UtcNow
    };

    var l1Entry2 = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "l2_consolidated.parquet"),
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

    var entry = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "a.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-b",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "b.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      Date = DateTime.UtcNow.Date.AddDays(-1),
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

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "a1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-a",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "a2.parquet"),
      Level = StorageLevel.L2,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream-b",
      Date = DateTime.UtcNow.Date,
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

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "l1.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 100,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      Date = DateTime.UtcNow.Date,
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

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      Date = DateTime.UtcNow.Date,
      FilePath = Path.Combine(_testDirectory, "a.parquet"),
      Level = StorageLevel.L1,
      RowCount = 10,
      FileSizeBytes = 1000,
      AddedAt = DateTime.UtcNow
    });

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "stream",
      Date = DateTime.UtcNow.Date,
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

    var entry = new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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
    // Create initial catalog
    var manager1 = new CatalogManager(_options, _logger);
    await manager1.InitializeAsync();

    await manager1.AddFileAsync(new CatalogEntry {
      StreamName = "test-stream",
      Date = DateTime.UtcNow.Date,
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

    await manager.AddFileAsync(new CatalogEntry {
      StreamName = "old-stream",
      Date = DateTime.UtcNow.Date,
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
                    Date = DateTime.UtcNow.Date,
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
}