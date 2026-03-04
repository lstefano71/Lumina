using Lumina.Query;

using Xunit;

namespace Lumina.Tests.Query;

public class SqlValidatorTests
{
  [Fact]
  public void IsValidSelectQuery_WithValidSelect_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT * FROM my_stream";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithValidSelectWithWhere_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT * FROM my_stream WHERE level = 'error'";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithCTE_ReturnsTrue()
  {
    // Arrange
    var sql = "WITH cte AS (SELECT * FROM my_stream) SELECT * FROM cte";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithJoin_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT a.*, b.* FROM stream_a a JOIN stream_b b ON a.id = b.id";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithInsert_ReturnsFalse()
  {
    // Arrange
    var sql = "INSERT INTO my_stream VALUES (1, 'test')";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("INSERT", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithUpdate_ReturnsFalse()
  {
    // Arrange
    var sql = "UPDATE my_stream SET level = 'error'";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("UPDATE", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithDELETE_ReturnsFalse()
  {
    // Arrange
    var sql = "DELETE FROM my_stream WHERE id = 1";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("DELETE", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithDROP_ReturnsFalse()
  {
    // Arrange
    var sql = "DROP TABLE my_stream";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("DROP", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithCREATE_ReturnsFalse()
  {
    // Arrange
    var sql = "CREATE TABLE my_stream (id INT, name VARCHAR)";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("CREATE", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithALTER_ReturnsFalse()
  {
    // Arrange
    var sql = "ALTER TABLE my_stream ADD COLUMN new_col VARCHAR";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("ALTER", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithTRUNCATE_ReturnsFalse()
  {
    // Arrange
    var sql = "TRUNCATE TABLE my_stream";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("TRUNCATE", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithEmptySql_ReturnsFalse()
  {
    // Arrange
    var sql = "";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("empty", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithWhitespaceSql_ReturnsFalse()
  {
    // Arrange
    var sql = "   ";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("empty", error);
  }

  [Fact]
  public void IsValidSelectQuery_WithSubquery_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT * FROM (SELECT id, name FROM my_stream) AS subq";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithGroupBy_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT level, COUNT(*) FROM my_stream GROUP BY level";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithOrderBy_ReturnsTrue()
  {
    // Arrange
    var sql = "SELECT * FROM my_stream ORDER BY timestamp DESC LIMIT 100";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.True(isValid);
    Assert.Null(error);
  }

  [Fact]
  public void IsValidSelectQuery_WithNonSelectStatement_ReturnsFalse()
  {
    // Arrange
    var sql = "SHOW TABLES";

    // Act
    var (isValid, error) = SqlValidator.IsValidSelectQuery(sql);

    // Assert
    Assert.False(isValid);
    Assert.Contains("SELECT", error);
  }

  [Fact]
  public void IsAllowedReadFunction_WithReadParquet_ReturnsTrue()
  {
    // Act & Assert
    Assert.True(SqlValidator.IsAllowedReadFunction("read_parquet"));
    Assert.True(SqlValidator.IsAllowedReadFunction("READ_PARQUET")); // Case insensitive
  }

  [Fact]
  public void IsAllowedReadFunction_WithReadCsv_ReturnsTrue()
  {
    // Act & Assert
    Assert.True(SqlValidator.IsAllowedReadFunction("read_csv"));
    Assert.True(SqlValidator.IsAllowedReadFunction("read_csv_auto"));
  }

  [Fact]
  public void IsAllowedReadFunction_WithReadJson_ReturnsTrue()
  {
    // Act & Assert
    Assert.True(SqlValidator.IsAllowedReadFunction("read_json"));
    Assert.True(SqlValidator.IsAllowedReadFunction("read_json_auto"));
  }

  [Fact]
  public void IsAllowedReadFunction_WithUnknownFunction_ReturnsFalse()
  {
    // Act & Assert
    Assert.False(SqlValidator.IsAllowedReadFunction("write_csv"));
    Assert.False(SqlValidator.IsAllowedReadFunction("unknown_function"));
  }
}