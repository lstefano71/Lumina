using Lumina.Query;

using Xunit;

namespace Lumina.Tests.Query;

public class StreamTableMappingTests
{
  [Fact]
  public void GetCreateViewSql_WithFiles_GeneratesCorrectSql()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "my_stream",
      ParquetFiles = new List<string> { "/data/l1/my_stream/file1.parquet", "/data/l2/my_stream/file2.parquet" }
    };

    // Act
    var sql = mapping.GetCreateViewSql();

    // Assert
    Assert.Contains("CREATE VIEW IF NOT EXISTS my_stream", sql);
    Assert.Contains("read_parquet([", sql);
    Assert.Contains("file1.parquet", sql);
    Assert.Contains("file2.parquet", sql);
    Assert.Contains("union_by_name=true", sql);
  }

  [Fact]
  public void GetCreateViewSql_WithNoFiles_GeneratesEmptyView()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "empty_stream",
      ParquetFiles = Array.Empty<string>()
    };

    // Act
    var sql = mapping.GetCreateViewSql();

    // Assert
    Assert.Contains("CREATE VIEW IF NOT EXISTS empty_stream", sql);
    Assert.Contains("SELECT NULL LIMIT 0", sql);
  }

  [Fact]
  public void GetCreateViewSql_EscapesSingleQuotesInFilePaths()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "test_stream",
      ParquetFiles = new List<string> { "/data/path's/file.parquet" }
    };

    // Act
    var sql = mapping.GetCreateViewSql();

    // Assert
    Assert.Contains("path''s", sql); // Single quote should be escaped to two single quotes
  }

  [Fact]
  public void GetCreateViewSql_WithReservedKeywordName_EscapesIdentifier()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "select", // Reserved keyword
      ParquetFiles = new List<string> { "/data/file.parquet" }
    };

    // Act
    var sql = mapping.GetCreateViewSql();

    // Assert
    Assert.Contains("\"select\"", sql); // Should be quoted
  }

  [Fact]
  public void GetCreateViewSql_WithSpecialCharacters_EscapesIdentifier()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "my-stream", // Contains hyphen
      ParquetFiles = new List<string> { "/data/file.parquet" }
    };

    // Act
    var sql = mapping.GetCreateViewSql();

    // Assert
    Assert.Contains("\"my-stream\"", sql);
  }

  [Fact]
  public void GetDropViewSql_GeneratesCorrectSql()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "my_stream",
      ParquetFiles = new List<string>()
    };

    // Act
    var sql = mapping.GetDropViewSql();

    // Assert
    Assert.Equal("DROP VIEW IF EXISTS my_stream", sql);
  }

  [Fact]
  public void GetViewName_WithSchema_IncludesSchemaPrefix()
  {
    // Arrange
    var mapping = new StreamTableMapping {
      StreamName = "my_stream",
      ParquetFiles = new List<string>()
    };

    // Act
    var viewName = mapping.GetViewName("myschema");

    // Assert
    Assert.Equal("myschema.my_stream", viewName);
  }
}