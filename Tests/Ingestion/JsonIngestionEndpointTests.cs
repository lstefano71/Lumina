using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Ingestion.Endpoints;
using Lumina.Ingestion.Models;
using Lumina.Storage.Wal;

using Microsoft.AspNetCore.Http.HttpResults;

using Xunit;

namespace Lumina.Tests.Ingestion;

public class JsonIngestionEndpointTests : IAsyncDisposable
{
  private readonly string _tempDirectory;
  private readonly WalManager _walManager;
  private readonly WalHotBuffer _hotBuffer;

  public JsonIngestionEndpointTests()
  {
    _tempDirectory = Path.Combine(Path.GetTempPath(), "LuminaEndpointTests", Guid.NewGuid().ToString());
    Directory.CreateDirectory(_tempDirectory);

    var settings = new WalSettings {
      DataDirectory = _tempDirectory,
      MaxWalSizeBytes = 10 * 1024 * 1024,
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    _walManager = new WalManager(settings);
    _hotBuffer = new WalHotBuffer();
  }

  public async ValueTask DisposeAsync()
  {
    await _walManager.DisposeAsync();
    if (Directory.Exists(_tempDirectory)) {
      try { Directory.Delete(_tempDirectory, true); } catch { }
    }
    GC.SuppressFinalize(this);
  }

  // --- Single entry ---

  [Fact]
  public async Task HandleSingle_ValidRequest_ShouldReturnOk()
  {
    var request = new LogIngestRequest {
      Stream = "test-stream",
      Level = "info",
      Message = "Hello"
    };

    var result = await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<Ok<IngestResponse>>();
    var ok = (Ok<IngestResponse>)result;
    ok.Value!.Success.Should().BeTrue();
    ok.Value.EntriesAccepted.Should().Be(1);
  }

  [Fact]
  public async Task HandleSingle_EmptyStream_ShouldReturnBadRequest()
  {
    var request = new LogIngestRequest {
      Stream = "",
      Level = "info",
      Message = "Hello"
    };

    var result = await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<BadRequest<IngestResponse>>();
  }

  [Fact]
  public async Task HandleSingle_EmptyMessage_ShouldReturnBadRequest()
  {
    var request = new LogIngestRequest {
      Stream = "test-stream",
      Level = "info",
      Message = ""
    };

    var result = await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<BadRequest<IngestResponse>>();
  }

  [Fact]
  public async Task HandleSingle_ShouldActuallyPersistToWal()
  {
    var request = new LogIngestRequest {
      Stream = "persist-test",
      Level = "error",
      Message = "Persisted message"
    };

    await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    // Flush writer so entries are readable
    var writer = await _walManager.GetOrCreateWriterAsync("persist-test");
    await writer.FlushAsync();

    // Verify by reading back
    var entries = new List<LogEntry>();
    await foreach (var entry in _walManager.ReadEntriesAsync("persist-test")) {
      entries.Add(entry);
    }

    entries.Should().HaveCount(1);
    entries[0].Message.Should().Be("Persisted message");
  }

  // --- Batch ---

  [Fact]
  public async Task HandleBatch_ValidRequest_ShouldReturnOk()
  {
    var request = new BatchLogIngestRequest {
      Stream = "test-stream",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "test-stream", Level = "info", Message = "m1" },
                new LogIngestRequest { Stream = "test-stream", Level = "warn", Message = "m2" },
                new LogIngestRequest { Stream = "test-stream", Level = "error", Message = "m3" }
            }
    };

    var result = await JsonIngestionEndpoint.HandleBatch(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<Ok<IngestResponse>>();
    var ok = (Ok<IngestResponse>)result;
    ok.Value!.Success.Should().BeTrue();
    ok.Value.EntriesAccepted.Should().Be(3);
  }

  [Fact]
  public async Task HandleBatch_EmptyStream_ShouldReturnBadRequest()
  {
    var request = new BatchLogIngestRequest {
      Stream = "",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "", Level = "info", Message = "m1" }
            }
    };

    var result = await JsonIngestionEndpoint.HandleBatch(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<BadRequest<IngestResponse>>();
  }

  [Fact]
  public async Task HandleBatch_EmptyEntries_ShouldReturnBadRequest()
  {
    var request = new BatchLogIngestRequest {
      Stream = "test-stream",
      Entries = Array.Empty<LogIngestRequest>()
    };

    var result = await JsonIngestionEndpoint.HandleBatch(request, _walManager, _hotBuffer, CancellationToken.None);

    result.Should().BeOfType<BadRequest<IngestResponse>>();
  }

  [Fact]
  public async Task HandleBatch_MultiStreamEntries_ShouldWriteToEachStream()
  {
    var request = new BatchLogIngestRequest {
      Stream = "default-stream",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "stream-a", Level = "info", Message = "a1" },
                new LogIngestRequest { Stream = "stream-b", Level = "info", Message = "b1" },
                new LogIngestRequest { Stream = "stream-a", Level = "info", Message = "a2" }
            }
    };

    var result = await JsonIngestionEndpoint.HandleBatch(request, _walManager, _hotBuffer, CancellationToken.None);
    result.Should().BeOfType<Ok<IngestResponse>>();

    // Flush writers so entries are readable
    var writerA = await _walManager.GetOrCreateWriterAsync("stream-a");
    await writerA.FlushAsync();
    var writerB = await _walManager.GetOrCreateWriterAsync("stream-b");
    await writerB.FlushAsync();

    var aEntries = new List<LogEntry>();
    await foreach (var e in _walManager.ReadEntriesAsync("stream-a")) aEntries.Add(e);
    aEntries.Should().HaveCount(2);

    var bEntries = new List<LogEntry>();
    await foreach (var e in _walManager.ReadEntriesAsync("stream-b")) bEntries.Add(e);
    bEntries.Should().HaveCount(1);
  }

  [Fact]
  public async Task HandleSingle_WithAttributes_ShouldPersistAttributes()
  {
    var request = new LogIngestRequest {
      Stream = "attr-test",
      Level = "info",
      Message = "with attrs",
      Attributes = new Dictionary<string, object?> {
        ["env"] = "production",
        ["version"] = 42
      }
    };

    await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    var attrWriter = await _walManager.GetOrCreateWriterAsync("attr-test");
    await attrWriter.FlushAsync();

    var entries = new List<LogEntry>();
    await foreach (var entry in _walManager.ReadEntriesAsync("attr-test")) entries.Add(entry);

    entries[0].Attributes.Should().ContainKey("env");
  }

  [Fact]
  public async Task HandleSingle_WithTraceContext_ShouldPersist()
  {
    var request = new LogIngestRequest {
      Stream = "trace-test",
      Level = "info",
      Message = "traced",
      TraceId = "trace-abc",
      SpanId = "span-xyz",
      DurationMs = 150
    };

    await JsonIngestionEndpoint.HandleSingle(request, _walManager, _hotBuffer, CancellationToken.None);

    var traceWriter = await _walManager.GetOrCreateWriterAsync("trace-test");
    await traceWriter.FlushAsync();

    var entries = new List<LogEntry>();
    await foreach (var entry in _walManager.ReadEntriesAsync("trace-test")) entries.Add(entry);

    entries[0].TraceId.Should().Be("trace-abc");
    entries[0].SpanId.Should().Be("span-xyz");
    entries[0].DurationMs.Should().Be(150);
  }
}
