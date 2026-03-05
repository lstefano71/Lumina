using System.Text.RegularExpressions;

namespace Lumina.Query;

/// <summary>
/// Validates SQL queries for safety, ensuring only SELECT statements are allowed.
/// </summary>
public static class SqlValidator
{
  /// <summary>
  /// SQL keywords that indicate DDL/DML operations (blocked).
  /// </summary>
  private static readonly HashSet<string> BlockedKeywords = new(StringComparer.OrdinalIgnoreCase)
  {
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
        "REPLACE", "MERGE", "UPSERT", "GRANT", "REVOKE", "EXEC", "EXECUTE",
        "CALL", "IMPORT", "EXPORT", "COPY", "ATTACH", "DETACH"
    };

  /// <summary>
  /// Blocked statement prefixes.
  /// </summary>
  private static readonly string[] BlockedPrefixes =
  {
        "INSERT INTO",
        "UPDATE ",
        "DELETE FROM",
        "DROP TABLE",
        "DROP VIEW",
        "DROP SCHEMA",
        "DROP DATABASE",
        "CREATE TABLE",
        "CREATE VIEW",
        "CREATE SCHEMA",
        "CREATE DATABASE",
        "CREATE INDEX",
        "CREATE FUNCTION",
        "CREATE MACRO",
        "ALTER TABLE",
        "ALTER VIEW",
        "ALTER SCHEMA",
        "TRUNCATE TABLE",
        "TRUNCATE ",
        "REPLACE TABLE",
        "MERGE INTO",
        "COPY ",
        "ATTACH DATABASE",
        "DETACH DATABASE"
    };

  /// <summary>
  /// Allowed DuckDB read functions for external data access.
  /// </summary>
  private static readonly HashSet<string> AllowedReadFunctions = new(StringComparer.OrdinalIgnoreCase)
  {
        "read_parquet", "read_csv", "read_csv_auto", "read_json", "read_json_auto",
        "read_text", "read_blob", "read_xlsx", "read_ods", "read_sql", "read_sqlite",
        "read_mysql", "read_postgres", "read_iceberg", "read_delta"
    };

  /// <summary>
  /// Validates that a SQL query is a safe SELECT statement.
  /// </summary>
  /// <param name="sql">The SQL query to validate.</param>
  /// <returns>A tuple indicating if the query is valid and an error message if not.</returns>
  public static (bool IsValid, string? Error) IsValidSelectQuery(string sql)
  {
    if (string.IsNullOrWhiteSpace(sql)) {
      return (false, "SQL query cannot be empty");
    }

    // Normalize whitespace for analysis
    var normalizedSql = NormalizeSql(sql);
    var upperSql = normalizedSql.ToUpperInvariant();

    // Check for blocked statement prefixes
    foreach (var prefix in BlockedPrefixes) {
      if (upperSql.StartsWith(prefix.ToUpperInvariant())) {
        return (false, $"Blocked statement type: {prefix}. Only SELECT queries are allowed.");
      }
    }

    // Check for semicolons indicating multiple statements
    var statements = SplitStatements(normalizedSql);
    if (statements.Count > 1) {
      // Only allow multiple statements if ALL are SELECT
      foreach (var statement in statements) {
        var trimmed = statement.Trim();
        if (!string.IsNullOrWhiteSpace(trimmed)) {
          var (isValid, error) = IsSingleSelectStatement(trimmed);
          if (!isValid) {
            return (false, error);
          }
        }
      }
    } else {
      // Single statement validation
      var (isValid, error) = IsSingleSelectStatement(normalizedSql);
      if (!isValid) {
        return (false, error);
      }
    }

    // Check for blocked keywords in dangerous contexts
    var keywordError = CheckBlockedKeywordsInContext(normalizedSql);
    if (keywordError != null) {
      return (false, keywordError);
    }

    return (true, null);
  }

  /// <summary>
  /// Validates a single statement is a SELECT.
  /// </summary>
  private static (bool IsValid, string? Error) IsSingleSelectStatement(string sql)
  {
    var trimmed = sql.TrimStart();
    var upper = trimmed.ToUpperInvariant();

    // Must start with SELECT or WITH (CTE)
    if (!upper.StartsWith("SELECT") && !upper.StartsWith("WITH")) {
      return (false, "Query must be a SELECT statement. Only SELECT queries are allowed.");
    }

    // CTEs must end with a SELECT
    if (upper.StartsWith("WITH")) {
      // Find the final SELECT after all CTEs
      var lastSelectIndex = upper.LastIndexOf("SELECT");
      if (lastSelectIndex < 0) {
        return (false, "CTE must end with a SELECT statement.");
      }
    }

    return (true, null);
  }

  /// <summary>
  /// Checks for blocked keywords in dangerous contexts.
  /// </summary>
  private static string? CheckBlockedKeywordsInContext(string sql)
  {
    var tokens = TokenizeSql(sql);

    for (int i = 0; i < tokens.Count; i++) {
      var token = tokens[i];
      var upperToken = token.ToUpperInvariant();

      // Skip if this is inside a string literal or function call that's allowed
      if (IsInAllowedContext(tokens, i)) {
        continue;
      }

      // Check for blocked keywords
      if (BlockedKeywords.Contains(upperToken)) {
        // Some keywords might be valid in SELECT context (like column names)
        // But we'll be conservative and block them
        return $"SQL contains blocked keyword: {token}. Only SELECT queries are allowed.";
      }
    }

    return null;
  }

  /// <summary>
  /// Checks if a token at the given position is in an allowed context (string literal, etc.).
  /// </summary>
  private static bool IsInAllowedContext(List<string> tokens, int index)
  {
    // Check if we're inside a string literal
    int stringDepth = 0;
    for (int i = 0; i < index; i++) {
      if (tokens[i] == "'" && (i == 0 || tokens[i - 1] != "'")) {
        stringDepth = stringDepth == 0 ? 1 : 0;
      }
    }

    return stringDepth > 0;
  }

  /// <summary>
  /// Normalizes SQL by collapsing whitespace.
  /// </summary>
  private static string NormalizeSql(string sql)
  {
    var result = new System.Text.StringBuilder();
    bool lastWasWhitespace = false;
    bool inString = false;

    foreach (char c in sql) {
      if (c == '\'' && !inString) {
        inString = true;
        result.Append(c);
      } else if (c == '\'' && inString) {
        inString = false;
        result.Append(c);
      } else if (inString) {
        result.Append(c);
      } else if (char.IsWhiteSpace(c)) {
        if (!lastWasWhitespace) {
          result.Append(' ');
          lastWasWhitespace = true;
        }
      } else {
        result.Append(c);
        lastWasWhitespace = false;
      }
    }

    return result.ToString().Trim();
  }

  /// <summary>
  /// Splits SQL into statements by semicolons (respecting string literals).
  /// </summary>
  private static List<string> SplitStatements(string sql)
  {
    var statements = new List<string>();
    var current = new System.Text.StringBuilder();
    bool inString = false;

    foreach (char c in sql) {
      if (c == '\'' && !inString) {
        inString = true;
        current.Append(c);
      } else if (c == '\'' && inString) {
        inString = false;
        current.Append(c);
      } else if (c == ';' && !inString) {
        var stmt = current.ToString().Trim();
        if (!string.IsNullOrWhiteSpace(stmt)) {
          statements.Add(stmt);
        }
        current.Clear();
      } else {
        current.Append(c);
      }
    }

    var lastStmt = current.ToString().Trim();
    if (!string.IsNullOrWhiteSpace(lastStmt)) {
      statements.Add(lastStmt);
    }

    return statements;
  }

  /// <summary>
  /// Tokenizes SQL into keywords, identifiers, and operators.
  /// </summary>
  private static List<string> TokenizeSql(string sql)
  {
    var tokens = new List<string>();
    var current = new System.Text.StringBuilder();
    bool inString = false;
    bool inIdentifier = false;
    bool inNumber = false;

    foreach (char c in sql) {
      if (inString) {
        if (c == '\'') {
          current.Append(c);
          tokens.Add(current.ToString());
          current.Clear();
          inString = false;
        } else {
          current.Append(c);
        }
      } else if (c == '\'') {
        if (current.Length > 0) {
          tokens.Add(current.ToString());
          current.Clear();
        }
        current.Append(c);
        inString = true;
      } else if (char.IsLetter(c) || c == '_') {
        if (!inIdentifier && current.Length > 0 && !char.IsLetter(current[^1]) && current[^1] != '_') {
          tokens.Add(current.ToString());
          current.Clear();
        }
        current.Append(c);
        inIdentifier = true;
        inNumber = false;
      } else if (char.IsDigit(c)) {
        if (!inNumber && !inIdentifier && current.Length > 0) {
          tokens.Add(current.ToString());
          current.Clear();
        }
        current.Append(c);
        inNumber = true;
        inIdentifier = false;
      } else if (c == '.' && (inNumber || inIdentifier)) {
        current.Append(c);
      } else {
        if (current.Length > 0) {
          tokens.Add(current.ToString());
          current.Clear();
        }
        inIdentifier = false;
        inNumber = false;

        if (!char.IsWhiteSpace(c)) {
          tokens.Add(c.ToString());
        }
      }
    }

    if (current.Length > 0) {
      tokens.Add(current.ToString());
    }

    return tokens;
  }

  /// <summary>
  /// Checks if a function name is an allowed read function for external data.
  /// </summary>
  public static bool IsAllowedReadFunction(string functionName)
  {
    return AllowedReadFunctions.Contains(functionName);
  }

  // Matches: <identifier> IN '<tick expression>' where the tick expression contains
  // range operators (..), duration separators (;), or $ variables that indicate
  // a TICK interval rather than a normal SQL IN ('value') literal.
  private static readonly Regex TickInPattern = new(
      @"(?<col>[A-Za-z_][A-Za-z0-9_.]*|""[^""]+"")\s+[Ii][Nn]\s+'(?<tick>[^']*(?:\$|\.\.|;)[^']*)'",
      RegexOptions.Compiled | RegexOptions.CultureInvariant);

  /// <summary>
  /// Rewrites QuestDB-style TICK interval expressions into standard SQL BETWEEN clauses.
  /// <para>
  /// Matches patterns like <c>ts IN '$now - 5m..$now'</c> and rewrites them to
  /// <c>ts BETWEEN '2024-01-01 12:00:00.000000' AND '2024-01-01 12:05:00.000000'</c>.
  /// This runs <b>before</b> DuckDB sees the query, so Parquet min/max pushdown is preserved.
  /// </para>
  /// <para>Expressions that fail to parse are left untouched.</para>
  /// </summary>
  public static string RewriteTickIntervals(string sql)
    => RewriteTickIntervals(sql, DateTimeOffset.UtcNow, useEpochRewrite: false);

  /// <summary>
  /// Overload that accepts an explicit <paramref name="now"/> for deterministic testing.
  /// </summary>
  public static string RewriteTickIntervals(string sql, DateTimeOffset now)
    => RewriteTickIntervals(sql, now, useEpochRewrite: false);

  /// <summary>
  /// Overload that accepts an explicit <paramref name="now"/> and rewrite strategy.
  /// When <paramref name="useEpochRewrite"/> is true, emitted predicates use
  /// <c>epoch_us(...)</c> comparison for correctness-first behavior.
  /// </summary>
  public static string RewriteTickIntervals(string sql, DateTimeOffset now, bool useEpochRewrite)
  {
    if (string.IsNullOrWhiteSpace(sql))
      return sql;

    return TickInPattern.Replace(sql, match => {
      var col = match.Groups["col"].Value;
      var tick = match.Groups["tick"].Value;

      if (!TickExpressionParser.TryParse(tick, now,
              out IReadOnlyList<(DateTimeOffset Start, DateTimeOffset End)> intervals))
        return match.Value; // leave unrecognised expressions untouched

      if (intervals.Count == 1) {
        var start = TickExpressionParser.FormatTimestamp(intervals[0].Start);
        var end = TickExpressionParser.FormatTimestamp(intervals[0].End);
        if (useEpochRewrite)
          return $"(epoch_us(try_cast({col} AS TIMESTAMP_NS)) >= epoch_us(CAST('{start}' AS TIMESTAMP_NS)) AND epoch_us(try_cast({col} AS TIMESTAMP_NS)) < epoch_us(CAST('{end}' AS TIMESTAMP_NS)))";

        return $"{col} BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}'";
      }

      // Multiple intervals → parenthesised OR chain
      var parts = new System.Text.StringBuilder();
      parts.Append('(');
      for (int i = 0; i < intervals.Count; i++) {
        if (i > 0) parts.Append(" OR ");
        var s = TickExpressionParser.FormatTimestamp(intervals[i].Start);
        var e = TickExpressionParser.FormatTimestamp(intervals[i].End);
        if (useEpochRewrite)
          parts.Append($"(epoch_us(try_cast({col} AS TIMESTAMP_NS)) >= epoch_us(CAST('{s}' AS TIMESTAMP_NS)) AND epoch_us(try_cast({col} AS TIMESTAMP_NS)) < epoch_us(CAST('{e}' AS TIMESTAMP_NS)))");
        else
          parts.Append($"{col} BETWEEN TIMESTAMP '{s}' AND TIMESTAMP '{e}'");
      }
      parts.Append(')');
      return parts.ToString();
    });
  }

  // Matches: FROM/JOIN 'stream-name' (single-quoted identifier in a table reference position)
  private static readonly Regex SingleQuotedTablePattern = new(
      @"(?<=[Ff][Rr][Oo][Mm]|[Jj][Oo][Ii][Nn])\s+'([^']+)'",
      RegexOptions.Compiled | RegexOptions.CultureInvariant);

  /// <summary>
  /// Rewrites single-quoted stream names in FROM/JOIN clauses to double-quoted SQL identifiers.
  /// e.g. <c>FROM 'my-stream'</c> → <c>FROM "my-stream"</c>
  /// This allows users to write natural stream-name syntax without worrying
  /// about DuckDB interpreting the single-quoted value as a file-path literal.
  /// </summary>
  public static string RewriteSingleQuotedIdentifiers(string sql)
  {
    return SingleQuotedTablePattern.Replace(sql, m => {
      // Preserve the original whitespace between the keyword and the name
      var leading = m.Value[..m.Value.IndexOf('\'')];
      var name = m.Groups[1].Value.Replace("\"", "\"\""); // escape any embedded double-quotes
      return $"{leading}\"{name}\"";
    });
  }
}