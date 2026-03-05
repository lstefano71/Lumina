using System.Globalization;
using System.Text.RegularExpressions;

namespace Lumina.Query;

/// <summary>
/// Parses a subset of the QuestDB TICK interval syntax and resolves it to
/// one or more concrete <see cref="DateTimeOffset"/> start/end intervals.
///
/// <para><b>Supported grammar:</b></para>
/// <list type="bullet">
///   <item><b>Variables:</b> <c>$now</c>, <c>$today</c>, <c>$yesterday</c>, <c>$tomorrow</c></item>
///   <item><b>ISO-8601 literals:</b> <c>2024-01-15T09:30:00</c>, <c>2024-01-15</c></item>
///   <item><b>Arithmetic:</b> <c>$now - 5m</c>, <c>$today + 2h30m</c></item>
///   <item><b>Ranges:</b> <c>$now - 1h..$now</c></item>
///   <item><b>Durations:</b> <c>$now - 1h;30m</c></item>
///   <item><b>Bracket expansion:</b> <c>2024-01-[10,15,20]</c>, <c>2024-01-[10..15]</c></item>
///   <item><b>Cartesian product:</b> <c>2024-[01,06]-[10,15]</c> (multiple brackets)</item>
///   <item><b>Date lists:</b> <c>[2024-01-15, 2024-03-20]</c>, <c>[$today, $yesterday]</c></item>
///   <item><b>Units:</b> <c>y</c>, <c>M</c>, <c>w</c>, <c>d</c>, <c>h</c>, <c>m</c>,
///         <c>s</c>, <c>T</c>/<c>ms</c>, <c>u</c>/<c>us</c></item>
/// </list>
///
/// <para>Overlapping intervals produced by bracket expansion are automatically merged.</para>
/// </summary>
public static class TickExpressionParser
{
  // ---------------------------------------------------------------------------
  // ISO-8601 date/datetime formats (full -> least precision)
  // ---------------------------------------------------------------------------
  private static readonly string[] DateFormats =
  {
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

  // Duration unit regex - captures compound durations like "1h30m10s"
  private static readonly Regex DurationPattern = new(
      @"(\d+)\s*(ms|us|[yMwdhmsTu])",
      RegexOptions.Compiled | RegexOptions.CultureInvariant);

  // ========================================================================
  //  Public API
  // ========================================================================

  /// <summary>
  /// Attempts to parse a tick expression string into one or more concrete
  /// time intervals.  Overlapping intervals are merged automatically.
  /// </summary>
  public static bool TryParse(
      string input,
      DateTimeOffset now,
      out IReadOnlyList<(DateTimeOffset Start, DateTimeOffset End)> intervals)
  {
    intervals = Array.Empty<(DateTimeOffset, DateTimeOffset)>();

    if (string.IsNullOrWhiteSpace(input))
      return false;

    var trimmed = input.Trim();

    try
    {
      var tokens = Lexer.Tokenize(trimmed);
      var pos = 0;
      var node = Parser.ParseTopLevel(tokens, ref pos);
      if (node == null || pos < tokens.Count)
        return false;

      var raw = Evaluator.Evaluate(node, now);
      if (raw.Count == 0)
        return false;

      intervals = MergeIntervals(raw);
      return true;
    }
    catch
    {
      return false;
    }
  }

  /// <summary>
  /// Convenience wrapper that returns only the first interval.
  /// Useful when the expression is known to produce a single interval.
  /// </summary>
  public static bool TryParseSingle(
      string input,
      DateTimeOffset now,
      out (DateTimeOffset Start, DateTimeOffset End) range)
  {
    range = default;
    if (!TryParse(input, now, out var intervals))
      return false;

    range = intervals[0];
    return true;
  }

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

    if (pos < trimmed.Length)
      return false;

    duration = dur;
    return true;
  }

  /// <summary>
  /// Formats a <see cref="DateTimeOffset"/> as a DuckDB-compatible timestamp string.
  /// </summary>
  public static string FormatTimestamp(DateTimeOffset ts)
    => ts.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

  // ========================================================================
  //  Interval merging
  // ========================================================================

  public static IReadOnlyList<(DateTimeOffset Start, DateTimeOffset End)> MergeIntervals(
      List<(DateTimeOffset Start, DateTimeOffset End)> raw)
  {
    if (raw.Count <= 1)
      return raw;

    raw.Sort((a, b) => a.Start.CompareTo(b.Start));

    var merged = new List<(DateTimeOffset Start, DateTimeOffset End)> { raw[0] };

    for (int i = 1; i < raw.Count; i++)
    {
      var last = merged[^1];
      var cur = raw[i];

      if (cur.Start <= last.End)
      {
        merged[^1] = (last.Start, cur.End > last.End ? cur.End : last.End);
      }
      else
      {
        merged.Add(cur);
      }
    }

    return merged;
  }

  // ========================================================================
  //  Shared duration consumer
  // ========================================================================

  internal static bool ConsumeDuration(string input, ref int pos, out TimeSpan duration)
  {
    duration = default;

    var match = DurationPattern.Match(input, pos);
    if (!match.Success || match.Index != pos)
      return false;

    var total = TimeSpan.Zero;
    int endPos = pos;

    while (match.Success && match.Index == endPos)
    {
      var val = long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
      var unit = match.Groups[2].Value;

      total += unit switch
      {
        "y" => TimeSpan.FromDays(val * 365),
        "M" => TimeSpan.FromDays(val * 30),
        "w" => TimeSpan.FromDays(val * 7),
        "d" => TimeSpan.FromDays(val),
        "h" => TimeSpan.FromHours(val),
        "m" => TimeSpan.FromMinutes(val),
        "s" => TimeSpan.FromSeconds(val),
        "T" or "ms" => TimeSpan.FromMilliseconds(val),
        "u" or "us" => TimeSpan.FromTicks(val * 10),
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

  // ########################################################################
  //  TOKEN TYPES
  // ########################################################################

  internal enum TokenKind
  {
    LBracket,       // [
    RBracket,       // ]
    Comma,          // ,
    DotDot,         // ..
    Semicolon,      // ;
    Plus,           // +
    Minus,          // -
    Variable,       // $now, $today, etc.
    IsoLiteral,     // 2024-01-15 or 2024-01-15T09:30:00.123
    Number,         // plain integer (used inside brackets: day/month numbers)
  }

  internal readonly record struct Token(TokenKind Kind, string Text);

  // ########################################################################
  //  LEXER
  // ########################################################################

  internal static class Lexer
  {
    public static List<Token> Tokenize(string input)
    {
      var tokens = new List<Token>();
      int pos = 0;

      while (pos < input.Length)
      {
        SkipWhitespace(input, ref pos);
        if (pos >= input.Length) break;

        char c = input[pos];

        if (c == '[') { tokens.Add(new Token(TokenKind.LBracket, "[")); pos++; continue; }
        if (c == ']') { tokens.Add(new Token(TokenKind.RBracket, "]")); pos++; continue; }
        if (c == ',') { tokens.Add(new Token(TokenKind.Comma, ",")); pos++; continue; }
        if (c == ';') { tokens.Add(new Token(TokenKind.Semicolon, ";")); pos++; continue; }
        if (c == '+') { tokens.Add(new Token(TokenKind.Plus, "+")); pos++; continue; }

        if (c == '-')
        {
          // Disambiguate: operator vs part of ISO date/continuation
          if (tokens.Count > 0)
          {
            var prev = tokens[^1].Kind;
            if (prev == TokenKind.Variable || prev == TokenKind.IsoLiteral ||
                prev == TokenKind.RBracket || prev == TokenKind.Number)
            {
              tokens.Add(new Token(TokenKind.Minus, "-"));
              pos++;
              continue;
            }
          }
          // Fall through to let ISO literal consume it
        }

        if (c == '.' && pos + 1 < input.Length && input[pos + 1] == '.')
        {
          tokens.Add(new Token(TokenKind.DotDot, ".."));
          pos += 2;
          continue;
        }

        // --- $variable ---
        if (c == '$')
        {
          var varToken = ConsumeVariable(input, ref pos);
          if (varToken != null) { tokens.Add(varToken.Value); continue; }
          return tokens;
        }

        // --- ISO literal or number ---
        if (char.IsDigit(c) || (c == '-' && pos + 1 < input.Length && char.IsDigit(input[pos + 1])))
        {
          if (TryConsumeIsoLiteral(input, ref pos, out var isoToken))
          {
            tokens.Add(isoToken);
            continue;
          }
          if (TryConsumeNumber(input, ref pos, out var numToken))
          {
            tokens.Add(numToken);
            continue;
          }
          return tokens;
        }

        // --- 'T' followed by time digits: e.g. T09:30 after a bracket ---
        if ((c == 'T' || c == 't') && pos + 1 < input.Length && char.IsDigit(input[pos + 1]))
        {
          if (TryConsumeTimeSuffix(input, ref pos, out var timeToken))
          {
            tokens.Add(timeToken);
            continue;
          }
        }

        // Unknown character → stop
        return tokens;
      }

      return tokens;
    }

    private static Token? ConsumeVariable(string input, ref int pos)
    {
      var remaining = input.AsSpan(pos);
      string[] vars = { "$yesterday", "$tomorrow", "$today", "$now" };

      foreach (var v in vars)
      {
        if (remaining.StartsWith(v, StringComparison.OrdinalIgnoreCase))
        {
          pos += v.Length;
          return new Token(TokenKind.Variable, v.ToLowerInvariant());
        }
      }
      return null;
    }

    private static bool TryConsumeIsoLiteral(string input, ref int pos, out Token token)
    {
      token = default;
      int maxLen = Math.Min(input.Length - pos, 35);
      var window = input.Substring(pos, maxLen);

      // Try full ISO date/datetime (greedy)
      for (int len = maxLen; len >= 10; len--)
      {
        var candidate = window[..len];
        if (DateTimeOffset.TryParseExact(
                candidate, DateFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
                out _))
        {
          token = new Token(TokenKind.IsoLiteral, candidate);
          pos += len;
          return true;
        }
      }

      // Partial date prefix ending in '-' before a bracket (for bracket expansion)
      // Match yyyy-MM- or yyyy-
      var partialMatch = Regex.Match(window, @"^\d{4}(-\d{2})?-");
      if (partialMatch.Success)
      {
        int pLen = partialMatch.Length;
        if (pos + pLen < input.Length && input[pos + pLen] == '[')
        {
          token = new Token(TokenKind.IsoLiteral, window[..pLen]);
          pos += pLen;
          return true;
        }
      }

      return false;
    }

    private static bool TryConsumeNumber(string input, ref int pos, out Token token)
    {
      token = default;
      int start = pos;
      while (pos < input.Length && char.IsDigit(input[pos]))
        pos++;

      if (pos == start)
        return false;

      // Greedily consume trailing letter chars for duration units (5m, 1h, 500ms, 100us)
      int unitStart = pos;
      while (pos < input.Length && char.IsLetter(input[pos]))
        pos++;

      token = new Token(TokenKind.Number, input[start..pos]);
      return true;
    }

    /// <summary>
    /// Consumes a time suffix starting with T, e.g. T09:30, T09:30:15.123.
    /// Produces an IsoLiteral token with the T prefix.
    /// </summary>
    private static bool TryConsumeTimeSuffix(string input, ref int pos, out Token token)
    {
      token = default;
      int start = pos;
      // Consume T
      pos++;
      // Consume digits and colons and dots (time components)
      while (pos < input.Length && (char.IsDigit(input[pos]) || input[pos] == ':' || input[pos] == '.'))
        pos++;

      if (pos <= start + 1)
      {
        pos = start;
        return false;
      }

      token = new Token(TokenKind.IsoLiteral, input[start..pos]);
      return true;
    }

    private static void SkipWhitespace(string s, ref int pos)
    {
      while (pos < s.Length && char.IsWhiteSpace(s[pos]))
        pos++;
    }
  }

  // ########################################################################
  //  AST NODE TYPES
  // ########################################################################

  internal abstract class AstNode { }

  internal sealed class LiteralNode(string text) : AstNode
  {
    public string Text { get; } = text;
  }

  internal sealed class VariableNode(string name) : AstNode
  {
    public string Name { get; } = name;
  }

  internal sealed class ArithmeticNode(AstNode @base, char sign, TimeSpan duration) : AstNode
  {
    public AstNode Base { get; } = @base;
    public char Sign { get; } = sign;
    public TimeSpan Duration { get; } = duration;
  }

  internal sealed class RangeNode(AstNode start, AstNode end) : AstNode
  {
    public AstNode Start { get; } = start;
    public AstNode End { get; } = end;
  }

  internal sealed class DurationSuffixNode(AstNode anchor, TimeSpan duration) : AstNode
  {
    public AstNode Anchor { get; } = anchor;
    public TimeSpan Duration { get; } = duration;
  }

  internal sealed class ListNode(List<AstNode> items) : AstNode
  {
    public List<AstNode> Items { get; } = items;
  }

  internal sealed class BracketExpansionNode(
      string prefix, List<AstNode> items, string suffix,
      BracketExpansionNode? nested = null) : AstNode
  {
    public string Prefix { get; } = prefix;
    public List<AstNode> Items { get; } = items;
    public string Suffix { get; } = suffix;
    public BracketExpansionNode? NestedExpansion { get; } = nested;
  }

  internal sealed class NumericRangeNode(int start, int end) : AstNode
  {
    public int Start { get; } = start;
    public int End { get; } = end;
  }

  // ########################################################################
  //  PARSER  (Recursive Descent)
  // ########################################################################

  internal static class Parser
  {
    public static AstNode? ParseTopLevel(List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count) return null;

      AstNode? node;

      // --- Top-level bracket: date list like [$today, $yesterday, 2024-01-15] ---
      if (Peek(tokens, pos) == TokenKind.LBracket)
      {
        node = ParseBracketedList(tokens, ref pos);
        if (node == null) return null;
      }
      // --- ISO literal that might contain bracket expansion ---
      else if (Peek(tokens, pos) == TokenKind.IsoLiteral)
      {
        node = ParseIsoWithOptionalBrackets(tokens, ref pos);
        if (node == null) return null;
        node = TryParseArithmetic(tokens, ref pos, node);
      }
      else if (Peek(tokens, pos) == TokenKind.Variable)
      {
        node = new VariableNode(tokens[pos].Text);
        pos++;
        node = TryParseArithmetic(tokens, ref pos, node);
      }
      else
      {
        return null;
      }

      // --- Range operator (..) ---
      if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.DotDot)
      {
        pos++;
        var right = ParseAnchorExpr(tokens, ref pos);
        if (right == null) return null;
        node = new RangeNode(node, right);
      }

      // --- Duration suffix (;) ---
      if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Semicolon)
      {
        pos++;
        if (!TryParseDurationFromTokens(tokens, ref pos, out var dur))
          return null;
        node = new DurationSuffixNode(node, dur);
      }

      return node;
    }

    private static AstNode? ParseAnchorExpr(List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count) return null;

      AstNode? node;

      if (Peek(tokens, pos) == TokenKind.Variable)
      {
        node = new VariableNode(tokens[pos].Text);
        pos++;
      }
      else if (Peek(tokens, pos) == TokenKind.IsoLiteral)
      {
        node = new LiteralNode(tokens[pos].Text);
        pos++;
      }
      else
      {
        return null;
      }

      return TryParseArithmetic(tokens, ref pos, node);
    }

    private static AstNode TryParseArithmetic(List<Token> tokens, ref int pos, AstNode node)
    {
      while (pos < tokens.Count)
      {
        var kind = Peek(tokens, pos);
        if (kind != TokenKind.Plus && kind != TokenKind.Minus)
          break;

        char sign = kind == TokenKind.Plus ? '+' : '-';
        pos++;

        if (!TryParseDurationFromTokens(tokens, ref pos, out var dur))
          break;

        node = new ArithmeticNode(node, sign, dur);
      }

      return node;
    }

    private static AstNode? ParseIsoWithOptionalBrackets(List<Token> tokens, ref int pos)
    {
      var isoText = tokens[pos].Text;
      pos++;

      if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.LBracket)
      {
        return ParseBracketExpansion(isoText, tokens, ref pos);
      }

      return new LiteralNode(isoText);
    }

    private static AstNode? ParseBracketExpansion(string prefix, List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.LBracket)
        return null;

      pos++; // consume [

      var items = ParseBracketItems(tokens, ref pos);
      if (items == null) return null;

      if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
        return null;
      pos++; // consume ]

      string suffix = "";
      BracketExpansionNode? nested = null;

      // After ], handle continuation: ]-NN, ]-[...], ]Ttime
      if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Minus)
      {
        // ]-Number or ]-[bracket]
        if (pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.Number)
        {
          var numText = tokens[pos + 1].Text;
          suffix = "-" + numText.PadLeft(2, '0');
          pos += 2;

          // Check for another bracket after the number: ]-NN-[...]
          if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.Minus &&
              pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.LBracket)
          {
            suffix += "-";
            pos++; // consume -
            nested = ParseBracketExpansionAsNested("", tokens, ref pos);
            if (nested == null) return null;
          }

          // Check for trailing T portion
          if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
          {
            var extra = tokens[pos].Text;
            if (extra.StartsWith("T", StringComparison.OrdinalIgnoreCase))
            {
              suffix += extra;
              pos++;
            }
          }
        }
        else if (pos + 1 < tokens.Count && Peek(tokens, pos + 1) == TokenKind.LBracket)
        {
          // ]-[bracket] (second bracket in Cartesian product)
          pos++; // consume -
          nested = ParseBracketExpansionAsNested("-", tokens, ref pos);
          if (nested == null) return null;
        }
      }

      // Check for trailing ISO literal after bracket (e.g., ]T09:30)
      if (nested == null && pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
      {
        var trailing = tokens[pos].Text;
        if (trailing.StartsWith("T", StringComparison.OrdinalIgnoreCase))
        {
          suffix += trailing;
          pos++;
        }
      }

      return new BracketExpansionNode(prefix, items, suffix, nested);
    }

    private static BracketExpansionNode? ParseBracketExpansionAsNested(
        string prefix, List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.LBracket)
        return null;

      pos++; // consume [

      var items = ParseBracketItems(tokens, ref pos);
      if (items == null) return null;

      if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
        return null;
      pos++; // consume ]

      string suffix = "";
      if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.IsoLiteral)
      {
        var trailing = tokens[pos].Text;
        if (trailing.StartsWith("T", StringComparison.OrdinalIgnoreCase))
        {
          suffix = trailing;
          pos++;
        }
      }

      return new BracketExpansionNode(prefix, items, suffix);
    }

    private static List<AstNode>? ParseBracketItems(List<Token> tokens, ref int pos)
    {
      var items = new List<AstNode>();

      while (pos < tokens.Count && Peek(tokens, pos) != TokenKind.RBracket)
      {
        if (items.Count > 0)
        {
          if (Peek(tokens, pos) != TokenKind.Comma)
            return null;
          pos++;
        }

        var item = ParseBracketItem(tokens, ref pos);
        if (item == null) return null;
        items.Add(item);
      }

      return items.Count > 0 ? items : null;
    }

    private static AstNode? ParseBracketItem(List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count) return null;

      var kind = Peek(tokens, pos);

      if (kind == TokenKind.Number)
      {
        int startNum = int.Parse(tokens[pos].Text, CultureInfo.InvariantCulture);
        pos++;

        if (pos < tokens.Count && Peek(tokens, pos) == TokenKind.DotDot)
        {
          pos++;
          if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.Number)
            return null;

          int endNum = int.Parse(tokens[pos].Text, CultureInfo.InvariantCulture);
          pos++;
          return new NumericRangeNode(startNum, endNum);
        }

        return new LiteralNode(startNum.ToString(CultureInfo.InvariantCulture));
      }

      if (kind == TokenKind.IsoLiteral)
      {
        var node = new LiteralNode(tokens[pos].Text);
        pos++;
        return TryParseArithmetic(tokens, ref pos, node) as AstNode;
      }

      if (kind == TokenKind.Variable)
      {
        AstNode node = new VariableNode(tokens[pos].Text);
        pos++;
        return TryParseArithmetic(tokens, ref pos, node);
      }

      return null;
    }

    private static AstNode? ParseBracketedList(List<Token> tokens, ref int pos)
    {
      pos++; // consume [

      var items = new List<AstNode>();

      while (pos < tokens.Count && Peek(tokens, pos) != TokenKind.RBracket)
      {
        if (items.Count > 0)
        {
          if (Peek(tokens, pos) != TokenKind.Comma)
            return null;
          pos++;
        }

        var item = ParseDateListEntry(tokens, ref pos);
        if (item == null) return null;
        items.Add(item);
      }

      if (pos >= tokens.Count || Peek(tokens, pos) != TokenKind.RBracket)
        return null;
      pos++; // consume ]

      if (items.Count == 0) return null;

      return new ListNode(items);
    }

    private static AstNode? ParseDateListEntry(List<Token> tokens, ref int pos)
    {
      if (pos >= tokens.Count) return null;

      var kind = Peek(tokens, pos);

      if (kind == TokenKind.Variable)
      {
        AstNode node = new VariableNode(tokens[pos].Text);
        pos++;
        return TryParseArithmetic(tokens, ref pos, node);
      }

      if (kind == TokenKind.IsoLiteral)
      {
        return ParseIsoWithOptionalBrackets(tokens, ref pos);
      }

      return null;
    }

    private static bool TryParseDurationFromTokens(List<Token> tokens, ref int pos, out TimeSpan dur)
    {
      dur = default;
      if (pos >= tokens.Count) return false;

      // Gather consecutive non-structural tokens as duration text
      var sb = new System.Text.StringBuilder();
      int startPos = pos;

      while (pos < tokens.Count)
      {
        var k = tokens[pos].Kind;
        if (k == TokenKind.DotDot || k == TokenKind.Semicolon ||
            k == TokenKind.LBracket || k == TokenKind.RBracket ||
            k == TokenKind.Comma || k == TokenKind.Plus || k == TokenKind.Minus)
          break;

        sb.Append(tokens[pos].Text);
        pos++;
      }

      if (sb.Length == 0)
      {
        pos = startPos;
        return false;
      }

      var durationText = sb.ToString();
      int dPos = 0;
      if (!ConsumeDuration(durationText, ref dPos, out dur) || dPos < durationText.Length)
      {
        pos = startPos;
        return false;
      }

      return true;
    }

    private static TokenKind? Peek(List<Token> tokens, int pos)
      => pos < tokens.Count ? tokens[pos].Kind : null;
  }

  // ########################################################################
  //  EVALUATOR - walks AST, returns intervals
  // ########################################################################

  internal static class Evaluator
  {
    public static List<(DateTimeOffset Start, DateTimeOffset End)> Evaluate(AstNode node, DateTimeOffset now)
    {
      switch (node)
      {
        case RangeNode rn:
        {
          var starts = EvaluateAnchors(rn.Start, now);
          var ends = EvaluateAnchors(rn.End, now);
          var result = new List<(DateTimeOffset, DateTimeOffset)>();
          foreach (var s in starts)
            foreach (var e in ends)
              result.Add((s, e));
          return result;
        }

        case DurationSuffixNode dsn:
        {
          var anchors = EvaluateAnchors(dsn.Anchor, now);
          return anchors.Select(a => (a, a + dsn.Duration)).ToList();
        }

        case ListNode ln:
        {
          var result = new List<(DateTimeOffset, DateTimeOffset)>();
          foreach (var item in ln.Items)
          {
            var sub = Evaluate(item, now);
            result.AddRange(sub);
          }
          return result;
        }

        case BracketExpansionNode ben:
        {
          var anchors = ExpandBrackets(ben, now);
          return anchors.Select(a => (a, a)).ToList();
        }

        default:
        {
          var anchors = EvaluateAnchors(node, now);
          return anchors.Select(a => (a, a)).ToList();
        }
      }
    }

    private static List<DateTimeOffset> EvaluateAnchors(AstNode node, DateTimeOffset now)
    {
      switch (node)
      {
        case VariableNode vn:
          return new List<DateTimeOffset> { ResolveVariable(vn.Name, now) };

        case LiteralNode lit:
          if (TryParseIso(lit.Text, out var dto))
            return new List<DateTimeOffset> { dto };
          return new List<DateTimeOffset>();

        case ArithmeticNode an:
        {
          var bases = EvaluateAnchors(an.Base, now);
          return bases.Select(b => an.Sign == '+' ? b + an.Duration : b - an.Duration).ToList();
        }

        case ListNode ln:
        {
          var result = new List<DateTimeOffset>();
          foreach (var item in ln.Items)
            result.AddRange(EvaluateAnchors(item, now));
          return result;
        }

        case BracketExpansionNode ben:
          return ExpandBrackets(ben, now);

        default:
          return new List<DateTimeOffset>();
      }
    }

    private static List<DateTimeOffset> ExpandBrackets(BracketExpansionNode node, DateTimeOffset now)
    {
      var values = ExpandBracketItems(node.Items);
      var results = new List<DateTimeOffset>();

      foreach (var val in values)
      {
        var padded = val.ToString(CultureInfo.InvariantCulture).PadLeft(2, '0');
        var dateStr = node.Prefix + padded + node.Suffix;

        if (node.NestedExpansion != null)
        {
          var nested = new BracketExpansionNode(
              dateStr + node.NestedExpansion.Prefix,
              node.NestedExpansion.Items,
              node.NestedExpansion.Suffix,
              node.NestedExpansion.NestedExpansion);

          results.AddRange(ExpandBrackets(nested, now));
        }
        else
        {
          if (TryParseIso(dateStr, out var dto))
            results.Add(dto);
        }
      }

      return results;
    }

    private static List<int> ExpandBracketItems(List<AstNode> items)
    {
      var result = new List<int>();

      foreach (var item in items)
      {
        switch (item)
        {
          case LiteralNode lit:
            if (int.TryParse(lit.Text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n))
              result.Add(n);
            break;

          case NumericRangeNode nrn:
            for (int i = nrn.Start; i <= nrn.End; i++)
              result.Add(i);
            break;
        }
      }

      return result;
    }

    private static DateTimeOffset ResolveVariable(string name, DateTimeOffset now)
    {
      return name switch
      {
        "$now" => now,
        "$today" => new DateTimeOffset(now.Date, now.Offset),
        "$yesterday" => new DateTimeOffset(now.Date.AddDays(-1), now.Offset),
        "$tomorrow" => new DateTimeOffset(now.Date.AddDays(1), now.Offset),
        _ => now
      };
    }

    private static bool TryParseIso(string text, out DateTimeOffset result)
    {
      return DateTimeOffset.TryParseExact(
          text, DateFormats,
          CultureInfo.InvariantCulture,
          DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces,
          out result);
    }
  }
}
