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

  // -----------------------------------------------------------------------
  // RewriteSingleQuotedIdentifiers
  // -----------------------------------------------------------------------

  [Fact]
  public void RewriteSingleQuotedIdentifiers_FromClause_RewritesToDoubleQuotes()
  {
    var sql = "SELECT * FROM 'test-stream' WHERE level = 'error'";
    var result = SqlValidator.RewriteSingleQuotedIdentifiers(sql);
    Assert.Contains("FROM \"test-stream\"", result);
    // String literal in WHERE must be left untouched
    Assert.Contains("level = 'error'", result);
  }

  [Fact]
  public void RewriteSingleQuotedIdentifiers_JoinClause_RewritesToDoubleQuotes()
  {
    var sql = "SELECT * FROM 'stream-a' JOIN 'stream-b' ON 1=1";
    var result = SqlValidator.RewriteSingleQuotedIdentifiers(sql);
    Assert.Contains("FROM \"stream-a\"", result);
    Assert.Contains("JOIN \"stream-b\"", result);
  }

  [Fact]
  public void RewriteSingleQuotedIdentifiers_AlreadyDoubleQuoted_Unchanged()
  {
    var sql = "SELECT * FROM \"test-stream\" WHERE version IS NOT NULL";
    var result = SqlValidator.RewriteSingleQuotedIdentifiers(sql);
    Assert.Equal(sql, result);
  }

  [Fact]
  public void RewriteSingleQuotedIdentifiers_NoStreamReference_Unchanged()
  {
    var sql = "SELECT * FROM plain_table WHERE level = 'info'";
    var result = SqlValidator.RewriteSingleQuotedIdentifiers(sql);
    Assert.Equal(sql, result);
  }

  [Fact]
  public void RewriteSingleQuotedIdentifiers_CaseInsensitive_From()
  {
    var sql = "select * from 'my-stream'";
    var result = SqlValidator.RewriteSingleQuotedIdentifiers(sql);
    Assert.Contains("\"my-stream\"", result);
    Assert.DoesNotContain("'my-stream'", result);
  }

  // -----------------------------------------------------------------------
  // RewriteTickIntervals
  // -----------------------------------------------------------------------

  // Fixed clock for deterministic SQL rewrite tests
  private static readonly DateTimeOffset FixedNow =
      new(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

  [Fact]
  public void RewriteTickIntervals_SimpleRange_RewritesToBetween()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '$now - 5m..$now'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2025-06-15 11:55:00.000000' AND TIMESTAMP '2025-06-15 12:00:00.000000'", result);
    Assert.DoesNotContain("$now", result);
  }

  [Fact]
  public void RewriteTickIntervals_DurationSyntax_RewritesToBetween()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '$now - 1h;30m'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2025-06-15 11:00:00.000000' AND TIMESTAMP '2025-06-15 11:30:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_IsoDateDuration_RewritesToBetween()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '2026-03-05;24h'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2026-03-05 00:00:00.000000' AND TIMESTAMP '2026-03-06 00:00:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_BracketExpansionWithoutDollarOrDuration_Rewrites()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '2026-03-[05,06]'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("(ts BETWEEN TIMESTAMP '2026-03-05 00:00:00.000000' AND TIMESTAMP '2026-03-05 00:00:00.000000' OR ts BETWEEN TIMESTAMP '2026-03-06 00:00:00.000000' AND TIMESTAMP '2026-03-06 00:00:00.000000')", result);
  }

  [Fact]
  public void RewriteTickIntervals_IsoLiterals_RewritesToBetween()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '2025-01-10T09:00:00..2025-01-10T17:00:00'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2025-01-10 09:00:00.000000' AND TIMESTAMP '2025-01-10 17:00:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_DoubleQuotedColumnName_Works()
  {
    var sql = "SELECT * FROM logs WHERE \"Timestamp\" IN '$now - 10m..$now'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("\"Timestamp\" BETWEEN TIMESTAMP '2025-06-15 11:50:00.000000' AND TIMESTAMP '2025-06-15 12:00:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_VariablesCaseInsensitive()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '$TODAY..$NOW'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2025-06-15 00:00:00.000000' AND TIMESTAMP '2025-06-15 12:00:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_NoTickExpression_Unchanged()
  {
    var sql = "SELECT * FROM logs WHERE level = 'error'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Equal(sql, result);
  }

  [Fact]
  public void RewriteTickIntervals_InvalidTickExpression_LeftUntouched()
  {
    // This string contains $ but is not a valid tick expression
    var sql = "SELECT * FROM logs WHERE ts IN '$bogus'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Equal(sql, result);
  }

  [Fact]
  public void RewriteTickIntervals_MultipleTickExpressions_AllRewritten()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '$now - 1h..$now' AND created IN '$today..$now'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN TIMESTAMP '2025-06-15 11:00:00.000000' AND TIMESTAMP '2025-06-15 12:00:00.000000'", result);
    Assert.Contains("created BETWEEN TIMESTAMP '2025-06-15 00:00:00.000000' AND TIMESTAMP '2025-06-15 12:00:00.000000'", result);
  }

  [Fact]
  public void RewriteTickIntervals_CoexistsWithStringLiterals()
  {
    // The WHERE clause has both a tick expression and a normal string literal
    var sql = "SELECT * FROM logs WHERE ts IN '$now - 5m..$now' AND level = 'error'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow);

    Assert.Contains("ts BETWEEN", result);
    Assert.Contains("level = 'error'", result);
  }

  [Fact]
  public void RewriteTickIntervals_UseEpochRewrite_EmitsEpochPredicate()
  {
    var sql = "SELECT * FROM logs WHERE ts IN '$now - 5m..$now'";
    var result = SqlValidator.RewriteTickIntervals(sql, FixedNow, useEpochRewrite: true);

    Assert.Contains("(epoch_us(try_cast(ts AS TIMESTAMP_NS)) >= epoch_us(CAST('2025-06-15 11:55:00.000000' AS TIMESTAMP_NS)) AND epoch_us(try_cast(ts AS TIMESTAMP_NS)) < epoch_us(CAST('2025-06-15 12:00:00.000000' AS TIMESTAMP_NS)))", result);
  }
}