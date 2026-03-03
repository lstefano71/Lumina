using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Parquet;

using Xunit;

namespace Lumina.Tests.Storage;

public class SchemaResolverTests
{
  // --- Fixed columns ---

  [Fact]
  public void ResolveSchema_ShouldAlwaysIncludeFixedColumns()
  {
    var entries = new[] { CreateEntry() };

    var schema = SchemaResolver.ResolveSchema(entries);

    var names = schema.Select(c => c.Name).ToList();
    names.Should().Contain("stream");
    names.Should().Contain("timestamp");
    names.Should().Contain("level");
    names.Should().Contain("message");
    names.Should().Contain("trace_id");
    names.Should().Contain("span_id");
    names.Should().Contain("duration_ms");
  }

  [Fact]
  public void ResolveSchema_FixedColumns_ShouldHaveCorrectTypes()
  {
    var schema = SchemaResolver.ResolveSchema(new[] { CreateEntry() });

    FindColumn(schema, "stream").Type.Should().Be(SchemaType.String);
    FindColumn(schema, "timestamp").Type.Should().Be(SchemaType.Timestamp);
    FindColumn(schema, "level").Type.Should().Be(SchemaType.String);
    FindColumn(schema, "message").Type.Should().Be(SchemaType.String);
    FindColumn(schema, "duration_ms").Type.Should().Be(SchemaType.Int32);
  }

  [Fact]
  public void ResolveSchema_FixedColumns_StreamAndTimestamp_NotNullable()
  {
    var schema = SchemaResolver.ResolveSchema(new[] { CreateEntry() });

    FindColumn(schema, "stream").IsNullable.Should().BeFalse();
    FindColumn(schema, "timestamp").IsNullable.Should().BeFalse();
    FindColumn(schema, "level").IsNullable.Should().BeFalse();
    FindColumn(schema, "message").IsNullable.Should().BeFalse();
  }

  // --- Dynamic attribute columns ---

  [Fact]
  public void ResolveSchema_ShouldIncludeCommonAttributeColumns()
  {
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> {
          ["http_status"] = 200,
          ["duration"] = 1.5
        })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var names = schema.Select(c => c.Name).ToList();

    names.Should().Contain("http_status");
    names.Should().Contain("duration");
  }

  [Fact]
  public void ResolveSchema_DynamicColumns_ShouldBeNullable()
  {
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> {
          ["tag"] = "value"
        })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var tagCol = FindColumn(schema, "tag");

    tagCol.Should().NotBeNull();
    tagCol.IsNullable.Should().BeTrue();
  }

  // --- Type promotion ---

  [Fact]
  public void ResolveSchema_IntVsString_ShouldPromoteToString()
  {
    var entries = new[]
    {
            CreateEntry(attributes: new Dictionary<string, object?> { ["key"] = 42 }),
            CreateEntry(attributes: new Dictionary<string, object?> { ["key"] = "hello" })
        };

    // Need enough entries so "key" is not pushed to overflow
    var bulked = Enumerable.Repeat(entries, 10).SelectMany(x => x).ToArray();
    var schema = SchemaResolver.ResolveSchema(bulked);

    var col = FindColumn(schema, "key");
    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.String);
  }

  [Fact]
  public void ResolveSchema_FloatVsInt_ShouldPromoteToDouble()
  {
    var entries = new[]
    {
            CreateEntry(attributes: new Dictionary<string, object?> { ["metric"] = 42 }),
            CreateEntry(attributes: new Dictionary<string, object?> { ["metric"] = 3.14f })
        };

    var bulked = Enumerable.Repeat(entries, 10).SelectMany(x => x).ToArray();
    var schema = SchemaResolver.ResolveSchema(bulked);

    var col = FindColumn(schema, "metric");
    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Double);
  }

  [Fact]
  public void ResolveSchema_Int32VsInt64_ShouldPromoteToInt64()
  {
    var entries = new[]
    {
            CreateEntry(attributes: new Dictionary<string, object?> { ["id"] = 42 }),
            CreateEntry(attributes: new Dictionary<string, object?> { ["id"] = (long)9999999999 })
        };

    var bulked = Enumerable.Repeat(entries, 10).SelectMany(x => x).ToArray();
    var schema = SchemaResolver.ResolveSchema(bulked);

    var col = FindColumn(schema, "id");
    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Int64);
  }

  [Fact]
  public void ResolveSchema_NullAndType_ShouldPromoteToType()
  {
    var entries = new[]
    {
            CreateEntry(attributes: new Dictionary<string, object?> { ["val"] = null }),
            CreateEntry(attributes: new Dictionary<string, object?> { ["val"] = 100 })
        };

    var bulked = Enumerable.Repeat(entries, 10).SelectMany(x => x).ToArray();
    var schema = SchemaResolver.ResolveSchema(bulked);

    var col = FindColumn(schema, "val");
    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Int32);
  }

  // --- Overflow / _meta ---

  [Fact]
  public void ResolveSchema_RareKeys_ShouldGoToMetaOverflow()
  {
    // Create entries where "rare_key" appears in only 1 out of many entries (< 10%)
    var entries = new List<LogEntry>();
    for (int i = 0; i < 100; i++) {
      var attrs = new Dictionary<string, object?> { ["common"] = "yes" };
      if (i == 0) attrs["rare_key"] = "rare_value";
      entries.Add(CreateEntry(attributes: attrs));
    }

    var schema = SchemaResolver.ResolveSchema(entries);
    var names = schema.Select(c => c.Name).ToList();

    names.Should().NotContain("rare_key", "rare keys should go to _meta overflow");
    names.Should().Contain("_meta");
  }

  [Fact]
  public void ResolveSchema_MetaColumn_ShouldBeJsonAndOverflow()
  {
    var entries = new List<LogEntry>();
    for (int i = 0; i < 100; i++) {
      var attrs = new Dictionary<string, object?> { ["common"] = "yes" };
      if (i == 0) attrs["rare_key"] = "rare_value";
      entries.Add(CreateEntry(attributes: attrs));
    }

    var schema = SchemaResolver.ResolveSchema(entries);
    var meta = FindColumn(schema, "_meta");

    meta.Should().NotBeNull();
    meta.Type.Should().Be(SchemaType.Json);
    meta.IsOverflow.Should().BeTrue();
    meta.IsNullable.Should().BeTrue();
  }

  [Fact]
  public void ResolveSchema_NoOverflowKeys_ShouldNotHaveMetaColumn()
  {
    // All entries have the same keys at high frequency
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> {
          ["key_a"] = "a",
          ["key_b"] = "b"
        })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var meta = schema.FirstOrDefault(c => c.Name == "_meta");

    meta.Should().BeNull("all keys are common enough to be promoted");
  }

  // --- Edge cases ---

  [Fact]
  public void ResolveSchema_EmptyEntries_ShouldReturnEmpty()
  {
    var schema = SchemaResolver.ResolveSchema(Array.Empty<LogEntry>());

    schema.Should().BeEmpty();
  }

  [Fact]
  public void ResolveSchema_NoAttributes_ShouldOnlyHaveFixedColumns()
  {
    var entries = new[]
    {
            CreateEntry(attributes: new Dictionary<string, object?>())
        };

    var schema = SchemaResolver.ResolveSchema(entries);

    schema.Select(c => c.Name).Should().BeEquivalentTo(
        new[] { "stream", "timestamp", "level", "message", "trace_id", "span_id", "duration_ms" });
  }

  [Fact]
  public void ResolveSchema_BooleanAttribute_ShouldBeBoolean()
  {
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> { ["flag"] = true })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var col = FindColumn(schema, "flag");

    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Boolean);
  }

  [Fact]
  public void ResolveSchema_DateTimeAttribute_ShouldBeTimestamp()
  {
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> { ["created"] = DateTime.UtcNow })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var col = FindColumn(schema, "created");

    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Timestamp);
  }

  [Fact]
  public void ResolveSchema_DictionaryAttribute_ShouldBeJson()
  {
    var entries = Enumerable.Range(0, 20).Select(i =>
        CreateEntry(attributes: new Dictionary<string, object?> {
          ["nested"] = new Dictionary<string, object?> { ["inner"] = "value" }
        })
    ).ToArray();

    var schema = SchemaResolver.ResolveSchema(entries);
    var col = FindColumn(schema, "nested");

    col.Should().NotBeNull();
    col.Type.Should().Be(SchemaType.Json);
  }

  [Fact]
  public void GetOverflowKeys_ShouldIdentifyRareKeys()
  {
    var entries = new List<LogEntry>();
    for (int i = 0; i < 100; i++) {
      var attrs = new Dictionary<string, object?> { ["common"] = "yes" };
      if (i == 0) attrs["rare"] = "value";
      entries.Add(CreateEntry(attributes: attrs));
    }

    var schemaKeys = new HashSet<string> { "common", "rare" };
    var overflow = SchemaResolver.GetOverflowKeys(entries, schemaKeys);

    overflow.Should().Contain("rare");
  }

  // --- Helpers ---

  private static ColumnSchema FindColumn(IReadOnlyList<ColumnSchema> schema, string name)
  {
    return schema.First(c => c.Name == name);
  }

  private static LogEntry CreateEntry(
      string stream = "test",
      Dictionary<string, object?>? attributes = null)
  {
    return new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "test",
      Attributes = attributes ?? new Dictionary<string, object?>()
    };
  }
}
