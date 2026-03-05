using System.Globalization;
using System.Text.RegularExpressions;

namespace Lumina.Query;

/// <summary>
/// Parses a subset of the QuestDB TICK interval syntax and resolves it to
/// concrete <see cref="DateTimeOffset"/> start/end boundaries.
///
/// <para><b>Supported grammar (subset):</b></para>
/// <list type="bullet">
///   <item><b>Variables:</b> <c>$now</c>, <c>$today</c>, <c>$yesterday</c>, <c>$tomorrow</c></item>
///   <item><b>ISO-8601 literals:</b> <c>2024-01-15T09:30:00</c>, <c>2024-01-15</c></item>
///   <item><b>Arithmetic:</b> <c>$now - 5m</c>, <c>$today + 2h30m</c></item>
///   <item><b>Ranges:</b> <c>$now - 1h..$now</c></item>
///   <item><b>Durations:</b> <c>$now - 1h;30m</c> (start at anchor, span forward for duration)</item>
///   <item><b>Units:</b> <c>y</c> (year), <c>M</c> (month), <c>w</c> (week),
///         <c>d</c> (day), <c>h</c> (hour), <c>m</c> (minute), <c>s</c> (second),
///         <c>T</c> or <c>ms</c> (millisecond), <c>u</c> or <c>us</c> (microsecond)</item>
/// </list>
///
/// <para>Examples:</para>
/// <code>
///   $now - 5m..$now           → last 5 minutes
///   $today..$now               → from midnight to now
///   $yesterday..$today         → full yesterday
///   2024-01-15T09:00..$now     → from a fixed start to now
///   $now - 1h;30m              → starting 1 hour ago, spanning 30 minutes
///   $now - 5m                  → point expression: [$now-5m, $now-5m] (degenerate range)
/// </code>
/// </summary>
public static class TickExpressionParser
{
  // ---------------------------------------------------------------------------
  // Duration unit pattern: captures compound durations like "1h30m10s"
  // ---------------------------------------------------------------------------
  private static readonly Regex DurationPattern = new(
      @"(\d+)\s*(ms|us|[yMwdhmsTu])",
      RegexOptions.Compiled | RegexOptions.CultureInvariant);

  // ---------------------------------------------------------------------------
  // ISO-8601 date/datetime pattern (loose, accepts common variants)
  // ---------------------------------------------------------------------------
  private static readonly string[] DateFormats =
  {
    // Full precision → least precision
    "yyyy-MM-dd'T'HH:mm:ss.fffffffK",
    "yyyy-MM-dd'T'HH:mm:ss.ffffffK",
    "yyyy-MM-dd'T'HH:mm:ss.fffffK",
    "yyyy-MM-dd'T'HH:mm:ss.ffffK",
    "yyyy-MM-dd'T'HH:mm:ss.fffK",
    "yyyy-MM-dd'T'HH:mm:ss.ffK",
    "yyyy-MM-dd'T'HH:mm:ss.fK",
    "yyyy-MM-dd'T'HH:mm:ssK",
    "yyyy-MM-dd'T'HH:mmK",
    "yyyy-MM-dd'T'HH:mm:ss.fffffff",
    "yyyy-MM-dd'T'HH:mm:ss.ffffff",
    "yyyy-MM-dd'T'HH:mm:ss.fffff",
    "yyyy-MM-dd'T'HH:mm:ss.ffff",
    "yyyy-MM-dd'T'HH:mm:ss.fff",
    "yyyy-MM-dd'T'HH:mm:ss.ff",
    "yyyy-MM-dd'T'HH:mm:ss.f",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm",
    "yyyy-MM-dd",
  };

  /// <summary>
  /// Attempts to parse a tick expression string into a concrete time range.
  /// </summary>
  /// <param name="input">The raw tick expression, e.g. <c>$now - 5m..$now</c>.</param>
  /// <param name="now">The reference "now" timestamp (normally <c>DateTimeOffset.UtcNow</c>).</param>
  /// <param name="range">When successful, the resolved [start, end] interval.</param>
  /// <returns><c>true</c> when the expression was parsed successfully.</returns>
  public static bool TryParse(
      string input,
      DateTimeOffset now,
      out (DateTimeOffset Start, DateTimeOffset End) range)
  {
    range = default;

    if (string.IsNullOrWhiteSpace(input))
      return false;

    var trimmed = input.Trim();

    // -----------------------------------------------------------------
    // 1. Split on ".." to detect a range expression
    // -----------------------------------------------------------------
    var rangeIndex = trimmed.IndexOf("..", StringComparison.Ordinal);
    if (rangeIndex >= 0) {
      var leftPart = trimmed[..rangeIndex].Trim();
      var rightPart = trimmed[(rangeIndex + 2)..].Trim();

      if (!TryParseAnchor(leftPart, now, out var start))
        return false;
      if (!TryParseAnchor(rightPart, now, out var end))
        return false;

      range = (start, end);
      return true;
    }

    // -----------------------------------------------------------------
    // 2. Split on ";" to detect a duration expression (anchor;duration)
    // -----------------------------------------------------------------
    var semiIndex = trimmed.IndexOf(';');
    if (semiIndex >= 0) {
      var anchorPart = trimmed[..semiIndex].Trim();
      var durationPart = trimmed[(semiIndex + 1)..].Trim();

      if (!TryParseAnchor(anchorPart, now, out var start))
        return false;
      if (!TryParseDuration(durationPart, out var span))
        return false;

      range = (start, start + span);
      return true;
    }

    // -----------------------------------------------------------------
    // 3. Single anchor expression (degenerate / point-in-time)
    // -----------------------------------------------------------------
    if (TryParseAnchor(trimmed, now, out var point)) {
      range = (point, point);
      return true;
    }

    return false;
  }

  // =========================================================================
  //  Anchor parsing  –  variable / literal  [+/- duration]*
  // =========================================================================

  /// <summary>
  /// Parses an anchor expression: a base (variable or ISO literal) optionally
  /// followed by one or more arithmetic offset terms.
  /// </summary>
  internal static bool TryParseAnchor(string expr, DateTimeOffset now, out DateTimeOffset result)
  {
    result = default;

    if (string.IsNullOrWhiteSpace(expr))
      return false;

    var str = expr.Trim();
    int pos = 0;

    // Tokenize into base + offset chunks.
    // We walk left-to-right: consume the base, then consume +/- duration pairs.
    if (!ConsumeBase(str, ref pos, now, out var baseValue))
      return false;

    var current = baseValue;

    // Process remaining +/- offsets
    SkipWhitespace(str, ref pos);
    while (pos < str.Length) {
      // Expect a sign
      var sign = str[pos];
      if (sign != '+' && sign != '-')
        return false; // unexpected token

      pos++;
      SkipWhitespace(str, ref pos);

      // Read the duration
      if (!ConsumeDuration(str, ref pos, out var dur))
        return false;

      current = sign == '+' ? current + dur : current - dur;
      SkipWhitespace(str, ref pos);
    }

    result = current;
    return true;
  }

  // =========================================================================
  //  Base consumer  –  $variable or ISO literal
  // =========================================================================

  private static bool ConsumeBase(string input, ref int pos, DateTimeOffset now, out DateTimeOffset value)
  {
    value = default;
    var remaining = input.AsSpan(pos);

    // --- Variables ---
    if (remaining.StartsWith("$now", StringComparison.OrdinalIgnoreCase)) {
      value = now;
      pos += 4;
      return true;
    }
    if (remaining.StartsWith("$today", StringComparison.OrdinalIgnoreCase)) {
      value = new DateTimeOffset(now.Date, now.Offset);
      pos += 6;
      return true;
    }
    if (remaining.StartsWith("$yesterday", StringComparison.OrdinalIgnoreCase)) {
      value = new DateTimeOffset(now.Date.AddDays(-1), now.Offset);
      pos += 10;
      return true;
    }
    if (remaining.StartsWith("$tomorrow", StringComparison.OrdinalIgnoreCase)) {
      value = new DateTimeOffset(now.Date.AddDays(1), now.Offset);
      pos += 9;
      return true;
    }

    // --- ISO-8601 literal ---
    // Greedily try progressively shorter substrings
    var str = remaining.ToString();
    for (int len = Math.Min(str.Length, 35); len >= 10; len--) {
      var candidate = str[..len];
      if (DateTimeOffset.TryParseExact(
              candidate, DateFormats,
              CultureInfo.InvariantCulture,
              DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
              out var dto)) {
        value = dto;
        pos += len;
        return true;
      }
    }

    return false;
  }

  // =========================================================================
  //  Duration consumer  –  compound "1h30m10s" segments
  // =========================================================================

  private static bool ConsumeDuration(string input, ref int pos, out TimeSpan duration)
  {
    duration = default;

    // We must consume at least one <number><unit> pair.
    var match = DurationPattern.Match(input, pos);
    if (!match.Success || match.Index != pos)
      return false;

    var total = TimeSpan.Zero;
    int endPos = pos;

    while (match.Success && match.Index == endPos) {
      var val = long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
      var unit = match.Groups[2].Value;

      total += unit switch {
        "y" => TimeSpan.FromDays(val * 365),
        "M" => TimeSpan.FromDays(val * 30),
        "w" => TimeSpan.FromDays(val * 7),
        "d" => TimeSpan.FromDays(val),
        "h" => TimeSpan.FromHours(val),
        "m" => TimeSpan.FromMinutes(val),
        "s" => TimeSpan.FromSeconds(val),
        "T" or "ms" => TimeSpan.FromMilliseconds(val),
        "u" or "us" => TimeSpan.FromTicks(val * 10), // 1 µs = 10 ticks
        _ => TimeSpan.Zero
      };

      endPos = match.Index + match.Length;
      match = match.NextMatch();
    }

    if (endPos == pos)
      return false;

    duration = total;
    pos = endPos;
    return true;
  }

  private static void SkipWhitespace(string s, ref int pos)
  {
    while (pos < s.Length && char.IsWhiteSpace(s[pos]))
      pos++;
  }

  // =========================================================================
  //  Public helper: parse a standalone duration string
  // =========================================================================

  /// <summary>
  /// Parses a standalone duration string such as <c>1h30m</c> or <c>500ms</c>.
  /// </summary>
  public static bool TryParseDuration(string input, out TimeSpan duration)
  {
    duration = default;
    if (string.IsNullOrWhiteSpace(input))
      return false;

    var trimmed = input.Trim();
    int pos = 0;
    if (!ConsumeDuration(trimmed, ref pos, out var dur))
      return false;

    // Ensure the entire string was consumed
    if (pos < trimmed.Length)
      return false;

    duration = dur;
    return true;
  }

  // =========================================================================
  //  Formatting helper for the SQL rewriter
  // =========================================================================

  /// <summary>
  /// Formats a <see cref="DateTimeOffset"/> as a DuckDB-compatible timestamp string.
  /// Uses microsecond precision to match DuckDB's TIMESTAMP type.
  /// </summary>
  public static string FormatTimestamp(DateTimeOffset ts)
    => ts.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
}
