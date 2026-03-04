using Lumina.Storage.Catalog;

using Microsoft.Extensions.Logging;

using Xunit;

namespace Lumina.Tests.Storage.Catalog;

public sealed class CatalogGarbageCollectorTests : IDisposable
{
  private readonly string _testDirectory;
  private readonly string _l1Directory;
  private readonly string _l2Directory;
  private readonly ILogger<CatalogGarbageCollector> _logger;
  private readonly CatalogGarbageCollector _gc;

  public CatalogGarbageCollectorTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"catalog_gc_test_{Guid.NewGuid():N}");
    _l1Directory = Path.Combine(_testDirectory, "L1");
    _l2Directory = Path.Combine(_testDirectory, "L2");

    Directory.CreateDirectory(_l1Directory);
    Directory.CreateDirectory(_l2Directory);

    _logger = LoggerFactory.Create(builder => builder.AddDebug()).CreateLogger<CatalogGarbageCollector>();
    _gc = new CatalogGarbageCollector(_logger);
  }

  public void Dispose()
  {
    if (Directory.Exists(_testDirectory)) {
      Directory.Delete(_testDirectory, recursive: true);
    }
  }

  [Fact]
  public async Task RunGcAsync_ShouldReturnZero_WhenNoOrphanedFiles()
  {
    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>()
    };

    var deletedCount = await _gc.RunGcAsync(catalog, _l1Directory, _l2Directory);

    Assert.Equal(0, deletedCount);
  }

  [Fact]
  public async Task FindOrphanedFiles_ShouldReturnEmpty_WhenNoFiles()
  {
    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>()
    };

    var orphaned = _gc.FindOrphanedFiles(catalog, _l1Directory, _l2Directory);

    Assert.Empty(orphaned);
  }

  [Fact]
  public async Task FindOrphanedFiles_ShouldDetectUnreferencedFiles()
  {
    // Create a file on disk
    var orphanedFile = Path.Combine(_l1Directory, "orphaned.parquet");
    await File.WriteAllTextAsync(orphanedFile, "test");

    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>()
    };

    var orphaned = _gc.FindOrphanedFiles(catalog, _l1Directory, _l2Directory);

    Assert.Single(orphaned);
    Assert.Contains(orphanedFile, orphaned);
  }

  [Fact]
  public async Task FindOrphanedFiles_ShouldNotIncludeCatalogedFiles()
  {
    var now = DateTime.UtcNow;
    // Create a file on disk
    var catalogedFile = Path.Combine(_l1Directory, "cataloged.parquet");
    await File.WriteAllTextAsync(catalogedFile, "test");

    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>
        {
          new()
          {
            StreamName = "test",
            MinTime = now.AddHours(-1),
            MaxTime = now,
            FilePath = Path.GetFullPath(catalogedFile),
            Level = StorageLevel.L1,
            RowCount = 100,
            FileSizeBytes = 4,
            AddedAt = DateTime.UtcNow
          }
        }
    };

    var orphaned = _gc.FindOrphanedFiles(catalog, _l1Directory, _l2Directory);

    Assert.Empty(orphaned);
  }

  [Fact]
  public async Task RunGcAsync_ShouldDeleteOrphanedFiles()
  {
    // Create orphaned files
    var orphanedFile1 = Path.Combine(_l1Directory, "orphaned1.parquet");
    var orphanedFile2 = Path.Combine(_l2Directory, "orphaned2.parquet");
    await File.WriteAllTextAsync(orphanedFile1, "test1");
    await File.WriteAllTextAsync(orphanedFile2, "test2");

    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>()
    };

    var deletedCount = await _gc.RunGcAsync(catalog, _l1Directory, _l2Directory);

    Assert.Equal(2, deletedCount);
    Assert.False(File.Exists(orphanedFile1));
    Assert.False(File.Exists(orphanedFile2));
  }

  [Fact]
  public async Task RunGcAsync_ShouldPreserveCatalogedFiles()
  {
    var now = DateTime.UtcNow;
    // Create cataloged file
    var catalogedFile = Path.Combine(_l1Directory, "cataloged.parquet");
    await File.WriteAllTextAsync(catalogedFile, "test");

    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>
        {
          new()
          {
            StreamName = "test",
            MinTime = now.AddHours(-1),
            MaxTime = now,
            FilePath = Path.GetFullPath(catalogedFile),
            Level = StorageLevel.L1,
            RowCount = 100,
            FileSizeBytes = 4,
            AddedAt = DateTime.UtcNow
          }
        }
    };

    var deletedCount = await _gc.RunGcAsync(catalog, _l1Directory, _l2Directory);

    Assert.Equal(0, deletedCount);
    Assert.True(File.Exists(catalogedFile));
  }

  [Fact]
  public async Task RunGcAsync_ShouldCleanupTempFiles()
  {
    // Create temp files
    var tempFile1 = Path.Combine(_l1Directory, "temp.tmp");
    var tempFile2 = Path.Combine(_l2Directory, "temp.tmp");
    await File.WriteAllTextAsync(tempFile1, "temp1");
    await File.WriteAllTextAsync(tempFile2, "temp2");

    var catalog = new StreamCatalog {
      Entries = new List<CatalogEntry>()
    };

    var deletedCount = await _gc.RunGcAsync(catalog, _l1Directory, _l2Directory);

    Assert.Equal(2, deletedCount);
    Assert.False(File.Exists(tempFile1));
    Assert.False(File.Exists(tempFile2));
  }
}