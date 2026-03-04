namespace Lumina.Query;

/// <summary>
/// Represents the mapping of a stream to its underlying Parquet files.
/// </summary>
public sealed class StreamTableMapping
{
  /// <summary>
  /// Gets the stream name (used as the table name in SQL).
  /// </summary>
  public required string StreamName { get; init; }

  /// <summary>
  /// Gets the list of Parquet file paths for this stream.
  /// </summary>
  public required IReadOnlyList<string> ParquetFiles { get; init; }

  /// <summary>
  /// Gets the CREATE VIEW SQL statement for this stream.
  /// </summary>
  /// <param name="schema">Optional schema name for the view.</param>
  /// <returns>The SQL statement to create a view for this stream.</returns>
  public string GetCreateViewSql(string? schema = null)
  {
    var files = ParquetFiles;
    if (files.Count == 0) {
      // Create an empty view with no rows
      return $"CREATE VIEW IF NOT EXISTS {GetViewName(schema)} AS SELECT * FROM (SELECT NULL LIMIT 0)";
    }

    var fileList = string.Join(", ", files.Select(f => $"'{EscapeSqlString(f)}'"));
    var viewName = GetViewName(schema);

    return $"CREATE VIEW IF NOT EXISTS {viewName} AS SELECT * FROM read_parquet([{fileList}], union_by_name=true)";
  }

  /// <summary>
  /// Gets the DROP VIEW SQL statement for this stream.
  /// </summary>
  /// <param name="schema">Optional schema name for the view.</param>
  /// <returns>The SQL statement to drop the view for this stream.</returns>
  public string GetDropViewSql(string? schema = null)
  {
    return $"DROP VIEW IF EXISTS {GetViewName(schema)}";
  }

  /// <summary>
  /// Gets the view name, optionally qualified with a schema.
  /// </summary>
  /// <param name="schema">Optional schema name.</param>
  /// <returns>The qualified or unqualified view name.</returns>
  public string GetViewName(string? schema = null)
  {
    // Escape the stream name to be a valid SQL identifier
    var escapedName = EscapeIdentifier(StreamName);
    return schema != null ? $"{schema}.{escapedName}" : escapedName;
  }

  /// <summary>
  /// Escapes a string for use in SQL string literals.
  /// </summary>
  private static string EscapeSqlString(string s) => s.Replace("'", "''");

  /// <summary>
  /// Escapes an identifier (table/view name) for use in SQL.
  /// Handles special characters and reserved keywords.
  /// </summary>
  private static string EscapeIdentifier(string name)
  {
    // If the name contains special characters or is a reserved keyword, quote it
    if (name.Any(c => !char.IsLetterOrDigit(c) && c != '_') ||
        IsReservedKeyword(name)) {
      // Use double quotes for identifiers (SQL standard)
      return $"\"{name.Replace("\"", "\"\"")}\"";
    }

    return name;
  }

  /// <summary>
  /// Checks if a name is a SQL reserved keyword.
  /// </summary>
  private static bool IsReservedKeyword(string name)
  {
    var upperName = name.ToUpperInvariant();
    var keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
            "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON", "AS", "ORDER", "BY",
            "GROUP", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL", "DISTINCT",
            "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
            "VIEW", "INDEX", "INTO", "VALUES", "SET", "WITH", "CASE", "WHEN",
            "THEN", "ELSE", "END", "BETWEEN", "LIKE", "EXISTS", "TRUE", "FALSE"
        };

    return keywords.Contains(upperName);
  }
}