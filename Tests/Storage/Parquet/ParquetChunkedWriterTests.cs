using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Parquet;

using Xunit;

namespace Lumina.Tests.Storage.Parquet;

/// <summary>
/// Tests for <see cref="ParquetWriter.WriteChunkedAsync"/>:
/// column-schema alignment across heterogeneous chunks and file-metadata embedding.
/// </summary>
public sealed class ParquetChunkedWriterTests : IDisposable
{
  private readonly string _testDirectory;

  public ParquetChunkedWriterTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"parquet_chunked_test_{Guid.NewGuid():N}");
    Directory.CreateDirectory(_testDirectory);
  }

  public void Dispose()
  {
    if (Directory.Exists(_testDirectory))
      Directory.Delete(_testDirectory, recursive: true);
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  private string TempFile(string name = "test.parquet") =>
      Path.Combine(_testDirectory, name);

  private static async IAsyncEnumerable<IReadOnlyList<LogEntry>> AsChunks(
      params IReadOnlyList<LogEntry>[] chunks)
  {
    foreach (var chunk in chunks)
      yield return chunk;
  }

  private static LogEntry MakeEntry(string msg, DateTime ts, Dictionary<string, object?> attrs) =>
      new() { Stream = "s", Timestamp = ts, Level = "info", Message = msg, Attributes = attrs };

  // ---------------------------------------------------------------------------
  //  Column-schema alignment (bug: ArgumentException on mismatched column order)
  // ---------------------------------------------------------------------------

  /// <summary>
  /// Exact reproduction of the production failure:
  ///   "cannot write this column, expected 'attr2', passed: 'workstation'"
  /// Chunk 1 promotes {attr1, attr2}; chunk 2 would independently resolve
  /// to a different column order / different key set.
  /// </summary>
  [Fact]
  public async Task WriteChunkedAsync_ChunksWithDifferentAttributeKeys_ShouldNotThrow()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    var chunk1 = Enumerable.Range(0, 20)
        .Select(i => MakeEntry($"c1-{i}", t.AddSeconds(i),
            new() { ["attr1"] = "v1", ["attr2"] = "v2" }))
        .ToList();

    var chunk2 = Enumerable.Range(0, 20)
        .Select(i => MakeEntry($"c2-{i}", t.AddHours(1).AddSeconds(i),
            new() { ["attr2"] = "v2", ["workstation"] = "ws1" }))
        .ToList();

    var act = async () =>
        await ParquetWriter.WriteChunkedAsync(AsChunks(chunk1, chunk2), TempFile());

    await act.Should().NotThrowAsync();
  }

  [Fact]
  public async Task WriteChunkedAsync_ChunksWithDifferentAttributeKeys_ShouldPreserveAllRowCounts()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    var chunk1 = Enumerable.Range(0, 15)
        .Select(i => MakeEntry($"c1-{i}", t.AddSeconds(i),
            new() { ["attr1"] = "v1", ["attr2"] = "v2" }))
        .ToList();

    var chunk2 = Enumerable.Range(0, 10)
        .Select(i => MakeEntry($"c2-{i}", t.AddHours(1).AddSeconds(i),
            new() { ["attr2"] = "v2", ["workstation"] = "ws1" }))
        .ToList();

    var path = TempFile();
    var written = await ParquetWriter.WriteChunkedAsync(AsChunks(chunk1, chunk2), path);

    written.Should().Be(25);
    var read = await ParquetReader.ReadEntriesAsync(path).ToListAsync();
    read.Should().HaveCount(25);
    read.Select(e => e.Message).Should()
        .Contain(m => m.StartsWith("c1-"))
        .And.Contain(m => m.StartsWith("c2-"));
  }

  [Fact]
  public async Task WriteChunkedAsync_ChunksWithThreeDifferentAttributeSets_ShouldNotThrow()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    var chunk1 = Enumerable.Range(0, 15)
        .Select(i => MakeEntry($"c1-{i}", t.AddSeconds(i),
            new() { ["attr1"] = "v1", ["attr2"] = "v2" }))
        .ToList();

    var chunk2 = Enumerable.Range(0, 15)
        .Select(i => MakeEntry($"c2-{i}", t.AddHours(1).AddSeconds(i),
            new() { ["attr2"] = "v2", ["attr3"] = "v3" }))
        .ToList();

    var chunk3 = Enumerable.Range(0, 15)
        .Select(i => MakeEntry($"c3-{i}", t.AddHours(2).AddSeconds(i),
            new() { ["attr1"] = "v1", ["attr3"] = "v3" }))
        .ToList();

    var path = TempFile();
    var act = async () =>
        await ParquetWriter.WriteChunkedAsync(AsChunks(chunk1, chunk2, chunk3), path);

    await act.Should().NotThrowAsync();
    var written = await ParquetReader.ReadEntriesAsync(path).ToListAsync();
    written.Should().HaveCount(45);
  }

  /// <summary>
  /// Attributes from later chunks that were NOT promoted in the master schema
  /// are routed to the _meta overflow column and deserialized back into Attributes on read.
  /// </summary>
  [Fact]
  public async Task WriteChunkedAsync_NewAttributeInLaterChunk_ShouldBeReadableViaMetaOverflow()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // chunk1 establishes master schema with attr1 + attr2
    var chunk1 = Enumerable.Range(0, 20)
        .Select(i => MakeEntry($"c1-{i}", t.AddSeconds(i),
            new() { ["attr1"] = "v1", ["attr2"] = "v2" }))
        .ToList();

    // chunk2 introduces newattr not in the master schema → goes to _meta
    var chunk2 = Enumerable.Range(0, 20)
        .Select(i => MakeEntry($"c2-{i}", t.AddHours(1).AddSeconds(i),
            new() { ["attr1"] = "v1", ["newattr"] = "extra" }))
        .ToList();

    var path = TempFile();
    await ParquetWriter.WriteChunkedAsync(AsChunks(chunk1, chunk2), path);

    var read = await ParquetReader.ReadEntriesAsync(path).ToListAsync();
    var chunk2Read = read.Where(e => e.Message.StartsWith("c2-")).ToList();

    chunk2Read.Should().HaveCount(20);
    // newattr is deserialized back from _meta JSON into Attributes
    chunk2Read.Should().AllSatisfy(e =>
        e.Attributes.Should().ContainKey("newattr"));
  }

  /// <summary>
  /// Columns present in the master schema but absent in a later chunk
  /// are null-filled; entries from that chunk should not have the key in Attributes.
  /// </summary>
  [Fact]
  public async Task WriteChunkedAsync_LaterChunkMissingPromotedAttribute_ShouldReadWithoutThatKey()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    var chunk1 = Enumerable.Range(0, 20)
        .Select(i => MakeEntry($"c1-{i}", t.AddSeconds(i),
            new() { ["attr1"] = "present" }))
        .ToList();

    // chunk2 has no attr1 at all
    var chunk2 = Enumerable.Range(0, 5)
        .Select(i => MakeEntry($"c2-{i}", t.AddHours(1).AddSeconds(i),
            new Dictionary<string, object?>()))
        .ToList();

    var path = TempFile();
    await ParquetWriter.WriteChunkedAsync(AsChunks(chunk1, chunk2), path);

    var read = await ParquetReader.ReadEntriesAsync(path).ToListAsync();
    var chunk2Read = read.Where(e => e.Message.StartsWith("c2-")).ToList();

    chunk2Read.Should().HaveCount(5);
    // attr1 is null-filled → ParquetReader skips null values → key is absent
    chunk2Read.Should().AllSatisfy(e => {
      if (e.Attributes.TryGetValue("attr1", out var v))
        v.Should().BeNull();
    });
  }

  // ---------------------------------------------------------------------------
  //  File metadata
  // ---------------------------------------------------------------------------

  [Fact]
  public async Task WriteChunkedAsync_WithFileMetadata_ShouldBeReadableViaStatisticsReader()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var chunk = Enumerable.Range(0, 5)
        .Select(i => MakeEntry($"m{i}", t.AddSeconds(i), new Dictionary<string, object?>()))
        .ToList();

    var path = TempFile("with-meta.parquet");
    await ParquetWriter.WriteChunkedAsync(
        AsChunks(chunk),
        path,
        fileMetadata: new Dictionary<string, string> {
          ["lumina.compaction_tier"] = "2",
          ["lumina.custom"] = "hello"
        });

    var meta = await ParquetStatisticsReader.ReadCustomMetadataAsync(path);
    meta.Should().ContainKey("lumina.compaction_tier").WhoseValue.Should().Be("2");
    meta.Should().ContainKey("lumina.custom").WhoseValue.Should().Be("hello");
  }

  [Fact]
  public async Task WriteChunkedAsync_WithoutFileMetadata_ShouldNotExposeCompactionTierKey()
  {
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var chunk = Enumerable.Range(0, 5)
        .Select(i => MakeEntry($"m{i}", t.AddSeconds(i), new Dictionary<string, object?>()))
        .ToList();

    var path = TempFile("no-meta.parquet");
    await ParquetWriter.WriteChunkedAsync(AsChunks(chunk), path);

    var meta = await ParquetStatisticsReader.ReadCustomMetadataAsync(path);
    meta.Should().NotContainKey("lumina.compaction_tier");
  }

  [Fact]
  public async Task ReadCustomMetadataAsync_MissingFile_ShouldReturnEmptyDictionary()
  {
    var meta = await ParquetStatisticsReader.ReadCustomMetadataAsync(
        Path.Combine(_testDirectory, "does-not-exist.parquet"));

    meta.Should().BeEmpty();
  }

  [Fact]
  public async Task WriteChunkedAsync_MetadataIsPreservedAcrossMultipleChunks()
  {
    // Metadata is set on the writer after the first chunk; verify it survives
    // even when multiple row groups are written.
    var t = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    var chunks = Enumerable.Range(0, 4)
        .Select(c => (IReadOnlyList<LogEntry>)Enumerable.Range(0, 10)
            .Select(i => MakeEntry($"c{c}-{i}", t.AddHours(c).AddSeconds(i),
                new Dictionary<string, object?>()))
            .ToList())
        .ToArray();

    var path = TempFile("multi-chunk-meta.parquet");
    var written = await ParquetWriter.WriteChunkedAsync(
        AsChunks(chunks),
        path,
        fileMetadata: new Dictionary<string, string> {
          ["lumina.compaction_tier"] = "3"
        });

    written.Should().Be(40);

    var meta = await ParquetStatisticsReader.ReadCustomMetadataAsync(path);
    meta.Should().ContainKey("lumina.compaction_tier").WhoseValue.Should().Be("3");
  }
}
