using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class WalHotBufferTests
{
  private readonly WalHotBuffer _buffer = new();

  [Fact]
  public void Append_SingleEntry_ShouldBeVisibleInSnapshot()
  {
    var entry = MakeEntry("test-stream", "hello");

    _buffer.Append("test-stream", "/wal/001.wal", 100, entry);

    var snapshot = _buffer.TakeSnapshot("test-stream");
    snapshot.Should().HaveCount(1);
    snapshot[0].LogEntry.Message.Should().Be("hello");
    snapshot[0].Offset.Should().Be(100);
    snapshot[0].WalFile.Should().Be("/wal/001.wal");
  }

  [Fact]
  public void Append_MultipleEntries_ShouldAllBeVisible()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s1", "/wal/001.wal", 200, MakeEntry("s1", "b"));
    _buffer.Append("s1", "/wal/001.wal", 300, MakeEntry("s1", "c"));

    _buffer.TakeSnapshot("s1").Should().HaveCount(3);
  }

  [Fact]
  public void AppendBatch_ShouldAddAllEntries()
  {
    var entries = new List<BufferedEntry> {
      new() { WalFile = "/wal/001.wal", Offset = 100, LogEntry = MakeEntry("s1", "a") },
      new() { WalFile = "/wal/001.wal", Offset = 200, LogEntry = MakeEntry("s1", "b") }
    };

    _buffer.AppendBatch("s1", entries);

    _buffer.TakeSnapshot("s1").Should().HaveCount(2);
  }

  [Fact]
  public void AppendBatch_EmptyList_ShouldNotIncrementVersion()
  {
    var versionBefore = _buffer.Version;
    _buffer.AppendBatch("s1", Array.Empty<BufferedEntry>());
    _buffer.Version.Should().Be(versionBefore);
  }

  [Fact]
  public void TakeSnapshot_UnknownStream_ShouldReturnEmpty()
  {
    _buffer.TakeSnapshot("nonexistent").Should().BeEmpty();
  }

  [Fact]
  public void EvictCompacted_ShouldRemoveOlderAndEqualEntries()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s1", "/wal/001.wal", 200, MakeEntry("s1", "b"));
    _buffer.Append("s1", "/wal/002.wal", 100, MakeEntry("s1", "c"));
    _buffer.Append("s1", "/wal/002.wal", 200, MakeEntry("s1", "d"));

    // Evict everything in 001.wal and up to offset 100 in 002.wal
    _buffer.EvictCompacted("s1", "/wal/002.wal", 100);

    var snapshot = _buffer.TakeSnapshot("s1");
    snapshot.Should().HaveCount(1);
    snapshot[0].LogEntry.Message.Should().Be("d");
    snapshot[0].Offset.Should().Be(200);
  }

  [Fact]
  public void EvictCompacted_OlderWalFile_ShouldRemoveAllInOlderFile()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s1", "/wal/001.wal", 200, MakeEntry("s1", "b"));
    _buffer.Append("s1", "/wal/002.wal", 50, MakeEntry("s1", "c"));

    _buffer.EvictCompacted("s1", "/wal/001.wal", 200);

    var snapshot = _buffer.TakeSnapshot("s1");
    snapshot.Should().HaveCount(1);
    snapshot[0].LogEntry.Message.Should().Be("c");
  }

  [Fact]
  public void EvictCompacted_NoEntries_ShouldNotThrow()
  {
    _buffer.EvictCompacted("nonexistent", "/wal/001.wal", 9999);
  }

  [Fact]
  public void Version_ShouldIncrement_OnAppendAndEvict()
  {
    var v0 = _buffer.Version;

    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    var v1 = _buffer.Version;
    v1.Should().BeGreaterThan(v0);

    _buffer.EvictCompacted("s1", "/wal/001.wal", 100);
    var v2 = _buffer.Version;
    v2.Should().BeGreaterThan(v1);
  }

  [Fact]
  public void GetStreamVersion_ShouldTrackPerStream()
  {
    _buffer.GetStreamVersion("s1").Should().Be(0);

    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    var v1 = _buffer.GetStreamVersion("s1");
    v1.Should().BeGreaterThan(0);

    // s2 should still be 0
    _buffer.GetStreamVersion("s2").Should().Be(0);
  }

  [Fact]
  public void GetBufferedStreams_ShouldReturnOnlyNonEmptyStreams()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s2", "/wal/001.wal", 100, MakeEntry("s2", "b"));

    var streams = _buffer.GetBufferedStreams();
    streams.Should().Contain("s1");
    streams.Should().Contain("s2");

    // Evict all from s1
    _buffer.EvictCompacted("s1", "/wal/999.wal", long.MaxValue);

    _buffer.GetBufferedStreams().Should().NotContain("s1");
  }

  [Fact]
  public void TotalEntries_ShouldReflectAllStreams()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s1", "/wal/001.wal", 200, MakeEntry("s1", "b"));
    _buffer.Append("s2", "/wal/001.wal", 100, MakeEntry("s2", "c"));

    _buffer.TotalEntries.Should().Be(3);
  }

  [Fact]
  public void TakeSnapshot_ShouldReturnCopy_NotLiveReference()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    var snapshot = _buffer.TakeSnapshot("s1");

    // Append more entries after the snapshot
    _buffer.Append("s1", "/wal/001.wal", 200, MakeEntry("s1", "b"));

    // Original snapshot should not have changed
    snapshot.Should().HaveCount(1);
  }

  [Fact]
  public void MultipleStreams_ShouldBeIndependent()
  {
    _buffer.Append("s1", "/wal/001.wal", 100, MakeEntry("s1", "a"));
    _buffer.Append("s2", "/wal/001.wal", 100, MakeEntry("s2", "b"));

    _buffer.TakeSnapshot("s1").Should().HaveCount(1);
    _buffer.TakeSnapshot("s2").Should().HaveCount(1);

    _buffer.EvictCompacted("s1", "/wal/999.wal", long.MaxValue);

    _buffer.TakeSnapshot("s1").Should().BeEmpty();
    _buffer.TakeSnapshot("s2").Should().HaveCount(1);
  }

  private static LogEntry MakeEntry(string stream, string message)
  {
    return new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = message
    };
  }
}
