using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Catalog;
using Lumina.Storage.Wal;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Query;

public class LiveQueryRefreshTests : IDisposable
{
  private readonly string _tempDir;
  private readonly DuckDbQueryService _queryService;
  private readonly WalHotBuffer _hotBuffer;

  public LiveQueryRefreshTests()
  {
    _tempDir = Path.Combine(Path.GetTempPath(), "LuminaLiveQuery", Guid.NewGuid().ToString());
    Directory.CreateDirectory(_tempDir);

    var compactionSettings = new CompactionSettings {
      L1Directory = Path.Combine(_tempDir, "l1"),
      L2Directory = Path.Combine(_tempDir, "l2"),
      CatalogDirectory = Path.Combine(_tempDir, "catalog"),
      CursorDirectory = Path.Combine(_tempDir, "cursors")
    };

    Directory.CreateDirectory(compactionSettings.L1Directory);
    Directory.CreateDirectory(compactionSettings.L2Directory);
    Directory.CreateDirectory(compactionSettings.CatalogDirectory);

    var catalogOptions = new CatalogOptions { CatalogDirectory = compactionSettings.CatalogDirectory };
    var catalogManager = new CatalogManager(
        catalogOptions,
        NullLogger<CatalogManager>.Instance);
    catalogManager.InitializeAsync().GetAwaiter().GetResult();

    var parquetManager = new ParquetManager(
        compactionSettings,
        NullLogger<ParquetManager>.Instance,
        catalogManager);

    var querySettings = new QuerySettings { LiveRefreshIntervalSeconds = 1 };
    _queryService = new DuckDbQueryService(querySettings, parquetManager, NullLogger<DuckDbQueryService>.Instance);
    _queryService.InitializeAsync().GetAwaiter().GetResult();

    _hotBuffer = new WalHotBuffer();
  }

  public void Dispose()
  {
    _queryService.Dispose();
    if (Directory.Exists(_tempDir)) {
      try { Directory.Delete(_tempDir, true); } catch { }
    }
    GC.SuppressFinalize(this);
  }

  [Fact]
  public async Task RefreshHotBuffer_ShouldMakeEntriesQueryable()
  {
    // Simulate writing entries to the hot buffer
    var entry = new LogEntry {
      Stream = "test-stream",
      Timestamp = new DateTime(2025, 6, 15, 10, 0, 0, DateTimeKind.Utc),
      Level = "info",
      Message = "Hello from hot buffer"
    };

    _hotBuffer.Append("test-stream", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("test-stream");

    // Materialize into DuckDB
    await _queryService.RefreshHotBufferAsync("test-stream", snapshot);

    // Query it back
    var result = await _queryService.ExecuteQueryAsync(
        "SELECT message, level FROM \"test-stream\" ORDER BY _t");

    result.RowCount.Should().Be(1);
    result.Rows[0]["message"].Should().Be("Hello from hot buffer");
    result.Rows[0]["level"].Should().Be("info");
  }

  [Fact]
  public async Task RefreshHotBuffer_MultipleEntries_ShouldAllBeQueryable()
  {
    var entries = new List<BufferedEntry>();
    for (int i = 0; i < 5; i++) {
      var e = new LogEntry {
        Stream = "multi-test",
        Timestamp = new DateTime(2025, 6, 15, 10, 0, i, DateTimeKind.Utc),
        Level = "info",
        Message = $"Entry {i}"
      };
      _hotBuffer.Append("multi-test", "/wal/001.wal", 100 + i * 50, e);
    }

    var snapshot = _hotBuffer.TakeSnapshot("multi-test");
    await _queryService.RefreshHotBufferAsync("multi-test", snapshot);

    var result = await _queryService.ExecuteQueryAsync(
        "SELECT COUNT(*) as cnt FROM \"multi-test\"");

    ((long)result.Rows[0]["cnt"]).Should().Be(5);
  }

  [Fact]
  public async Task ClearHotTable_ShouldRemoveAllEntries()
  {
    var entry = new LogEntry {
      Stream = "clear-test",
      Timestamp = DateTime.UtcNow,
      Level = "warn",
      Message = "will be cleared"
    };

    _hotBuffer.Append("clear-test", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("clear-test");
    await _queryService.RefreshHotBufferAsync("clear-test", snapshot);

    // Verify it's there
    var before = await _queryService.ExecuteQueryAsync(
        "SELECT COUNT(*) as cnt FROM \"clear-test\"");
    ((long)before.Rows[0]["cnt"]).Should().Be(1);

    // Clear
    await _queryService.ClearHotTableAsync("clear-test");

    // Rebuild view to reflect cleared table
    await _queryService.RebuildStreamViewAsync("clear-test");

    var after = await _queryService.ExecuteQueryAsync(
        "SELECT COUNT(*) as cnt FROM \"clear-test\"");
    ((long)after.Rows[0]["cnt"]).Should().Be(0);
  }

  [Fact]
  public async Task RefreshHotBuffer_WithAttributes_ShouldPromoteToTopLevelColumns()
  {
    // Attributes that appear in every entry (100%) must be promoted to top-level
    // columns so they are directly queryable — not buried in _meta JSON.
    var entry = new LogEntry {
      Stream = "attr-test",
      Timestamp = DateTime.UtcNow,
      Level = "debug",
      Message = "with attributes",
      Attributes = new Dictionary<string, object?> {
        ["env"] = "test",
        ["count"] = 42
      }
    };

    _hotBuffer.Append("attr-test", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("attr-test");
    await _queryService.RefreshHotBufferAsync("attr-test", snapshot);

    // Both attributes must be queryable as first-class columns.
    var result = await _queryService.ExecuteQueryAsync(
        "SELECT env, count FROM \"attr-test\"");

    result.RowCount.Should().Be(1);
    (result.Rows[0]["env"] as string).Should().Be("test");
    Convert.ToInt32(result.Rows[0]["count"]).Should().Be(42);
  }

  [Fact]
  public async Task RefreshHotBuffer_WithTraceContext_ShouldPersist()
  {
    var entry = new LogEntry {
      Stream = "trace-q-test",
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "traced entry",
      TraceId = "trace-123",
      SpanId = "span-456",
      DurationMs = 250
    };

    _hotBuffer.Append("trace-q-test", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("trace-q-test");
    await _queryService.RefreshHotBufferAsync("trace-q-test", snapshot);

    var result = await _queryService.ExecuteQueryAsync(
        "SELECT trace_id, span_id, duration_ms FROM \"trace-q-test\"");

    result.RowCount.Should().Be(1);
    result.Rows[0]["trace_id"].Should().Be("trace-123");
    result.Rows[0]["span_id"].Should().Be("span-456");
    result.Rows[0]["duration_ms"].Should().Be(250);
  }

  [Fact]
  public async Task RefreshHotBuffer_EmptySnapshot_ShouldNotFail()
  {
    await _queryService.RefreshHotBufferAsync("empty-test", Array.Empty<BufferedEntry>());

    // Should still be queryable (empty)
    var result = await _queryService.ExecuteQueryAsync(
        "SELECT COUNT(*) as cnt FROM \"empty-test\"");
    ((long)result.Rows[0]["cnt"]).Should().Be(0);
  }

  [Fact]
  public async Task RefreshHotBuffer_MessageWithSingleQuote_ShouldEscapeCorrectly()
  {
    var entry = new LogEntry {
      Stream = "escape-test",
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "It's a test with 'quotes'"
    };

    _hotBuffer.Append("escape-test", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("escape-test");
    await _queryService.RefreshHotBufferAsync("escape-test", snapshot);

    var result = await _queryService.ExecuteQueryAsync(
        "SELECT message FROM \"escape-test\"");

    result.RowCount.Should().Be(1);
    result.Rows[0]["message"].Should().Be("It's a test with 'quotes'");
  }

  [Fact]
  public void GetHotTableName_ShouldSanitizeStreamName()
  {
    DuckDbQueryService.GetHotTableName("my-stream").Should().Be("_hot_my_stream");
    DuckDbQueryService.GetHotTableName("app.logs").Should().Be("_hot_app_logs");
    DuckDbQueryService.GetHotTableName("simple").Should().Be("_hot_simple");
  }

  // ---------------------------------------------------------------------------
  // Stream discovery — hot-only streams (no Parquet files yet)
  // ---------------------------------------------------------------------------

  [Fact]
  public async Task HotOnlyStream_ShouldAppearInRegisteredStreams()
  {
    // A stream that only exists in the hot buffer (never compacted to Parquet)
    // must be visible via GetRegisteredStreams() so that the /v1/streams endpoint
    // can include it in its response.
    var entry = new LogEntry {
      Stream = "hot-only-stream",
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "live entry"
    };

    _hotBuffer.Append("hot-only-stream", "/wal/001.wal", 100, entry);
    var snapshot = _hotBuffer.TakeSnapshot("hot-only-stream");
    await _queryService.RefreshHotBufferAsync("hot-only-stream", snapshot);

    _queryService.GetRegisteredStreams().Should().Contain("hot-only-stream");
  }

  [Fact]
  public async Task HotOnlyStream_ShouldAppearInHotBufferList()
  {
    // GetBufferedStreams() is the second source the /v1/streams endpoint uses.
    // Confirming it returns the stream name after an Append.
    var entry = new LogEntry {
      Stream = "hot-buffer-stream",
      Timestamp = DateTime.UtcNow,
      Level = "debug",
      Message = "buffered"
    };

    _hotBuffer.Append("hot-buffer-stream", "/wal/001.wal", 1, entry);

    _hotBuffer.GetBufferedStreams().Should().Contain("hot-buffer-stream");
  }

  [Fact]
  public async Task HotOnlyStream_ShouldBeQueryableBeforeCompaction()
  {
    // Regression guard: /v1/query/sql must succeed for a stream that only has
    // hot-buffer data (i.e. the stream exists in DuckDB even before first compaction).
    var entries = Enumerable.Range(1, 5).Select(i => new LogEntry {
      Stream = "pre-compact-stream",
      Timestamp = DateTime.UtcNow.AddSeconds(-i),
      Level = "info",
      Message = $"entry {i}",
      Attributes = new Dictionary<string, object?> { ["version"] = i }
    }).ToList();

    foreach (var e in entries)
      _hotBuffer.Append("pre-compact-stream", "/wal/001.wal", e.Attributes["version"] is int v ? v : 0, e);

    var snapshot = _hotBuffer.TakeSnapshot("pre-compact-stream");
    await _queryService.RefreshHotBufferAsync("pre-compact-stream", snapshot);

    // The promoted attribute column "version" must be queryable directly.
    var result = await _queryService.ExecuteQueryAsync(
        "SELECT COUNT(*) as cnt FROM \"pre-compact-stream\" WHERE version IS NOT NULL");

    result.RowCount.Should().Be(1);
    ((long)result.Rows[0]["cnt"]).Should().Be(5);
  }

  [Fact]
  public async Task MultipleHotOnlyStreams_AllShouldAppearInRegisteredStreams()
  {
    var streamNames = new[] { "stream-alpha", "stream-beta", "stream-gamma" };

    foreach (var name in streamNames) {
      var entry = new LogEntry {
        Stream = name,
        Timestamp = DateTime.UtcNow,
        Level = "info",
        Message = $"entry for {name}"
      };
      _hotBuffer.Append(name, "/wal/001.wal", 1, entry);
      var snapshot = _hotBuffer.TakeSnapshot(name);
      await _queryService.RefreshHotBufferAsync(name, snapshot);
    }

    var registered = _queryService.GetRegisteredStreams();
    registered.Should().Contain(streamNames);
  }
}
