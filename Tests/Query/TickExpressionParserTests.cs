using Lumina.Query;

using Xunit;

namespace Lumina.Tests.Query;

public class TickExpressionParserTests
{
  // Fixed reference time for deterministic tests: 2025-06-15T12:00:00Z
  private static readonly DateTimeOffset Now =
      new(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);

  // -----------------------------------------------------------------------
  // Variable anchors
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_NowVariable_ResolvesToNow()
  {
    Assert.True(TickExpressionParser.TryParse("$now", Now, out var range));
    Assert.Equal(Now, range.Start);
    Assert.Equal(Now, range.End);
  }

  [Fact]
  public void Parse_TodayVariable_ResolvesToMidnight()
  {
    Assert.True(TickExpressionParser.TryParse("$today", Now, out var range));
    var expected = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  [Fact]
  public void Parse_YesterdayVariable_ResolvesToPreviousMidnight()
  {
    Assert.True(TickExpressionParser.TryParse("$yesterday", Now, out var range));
    var expected = new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  [Fact]
  public void Parse_TomorrowVariable_ResolvesToNextMidnight()
  {
    Assert.True(TickExpressionParser.TryParse("$tomorrow", Now, out var range));
    var expected = new DateTimeOffset(2025, 6, 16, 0, 0, 0, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  // -----------------------------------------------------------------------
  // ISO-8601 literal anchors
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_IsoDateOnly_ResolvesToMidnight()
  {
    Assert.True(TickExpressionParser.TryParse("2025-01-10", Now, out var range));
    Assert.Equal(new DateTimeOffset(2025, 1, 10, 0, 0, 0, TimeSpan.Zero), range.Start);
  }

  [Fact]
  public void Parse_IsoDateTimeFull_Resolves()
  {
    Assert.True(TickExpressionParser.TryParse("2025-01-10T09:30:15.123", Now, out var range));
    var expected = new DateTimeOffset(2025, 1, 10, 9, 30, 15, 123, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  [Fact]
  public void Parse_IsoDateTimeMinuteOnly_Resolves()
  {
    Assert.True(TickExpressionParser.TryParse("2025-03-20T14:45", Now, out var range));
    var expected = new DateTimeOffset(2025, 3, 20, 14, 45, 0, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  // -----------------------------------------------------------------------
  // Arithmetic offsets
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_NowMinus5Minutes_SubtractsCorrectly()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 5m", Now, out var range));
    Assert.Equal(Now.AddMinutes(-5), range.Start);
  }

  [Fact]
  public void Parse_NowPlus2Hours_AddsCorrectly()
  {
    Assert.True(TickExpressionParser.TryParse("$now + 2h", Now, out var range));
    Assert.Equal(Now.AddHours(2), range.Start);
  }

  [Fact]
  public void Parse_CompoundDuration_1h30m()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 1h30m", Now, out var range));
    Assert.Equal(Now.AddHours(-1).AddMinutes(-30), range.Start);
  }

  [Fact]
  public void Parse_MultipleOffsets_SubtractAndAdd()
  {
    // $now - 2h + 15m → subtract 2 hours, then add 15 minutes
    Assert.True(TickExpressionParser.TryParse("$now - 2h + 15m", Now, out var range));
    Assert.Equal(Now.AddHours(-2).AddMinutes(15), range.Start);
  }

  [Fact]
  public void Parse_IsoLiteralWithOffset()
  {
    Assert.True(TickExpressionParser.TryParse("2025-01-10T12:00:00 - 30m", Now, out var range));
    var expected = new DateTimeOffset(2025, 1, 10, 11, 30, 0, TimeSpan.Zero);
    Assert.Equal(expected, range.Start);
  }

  // -----------------------------------------------------------------------
  // Duration units coverage
  // -----------------------------------------------------------------------

  [Theory]
  [InlineData("$now - 1y", -365)]     // years → 365 days
  [InlineData("$now - 1M", -30)]      // months → 30 days
  [InlineData("$now - 1w", -7)]       // weeks → 7 days
  [InlineData("$now - 1d", -1)]       // days
  public void Parse_DayBasedUnits(string expr, int expectedDays)
  {
    Assert.True(TickExpressionParser.TryParse(expr, Now, out var range));
    Assert.Equal(Now.AddDays(expectedDays), range.Start);
  }

  [Fact]
  public void Parse_MillisecondUnit_T()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 500T", Now, out var range));
    Assert.Equal(Now.AddMilliseconds(-500), range.Start);
  }

  [Fact]
  public void Parse_MillisecondUnit_ms()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 500ms", Now, out var range));
    Assert.Equal(Now.AddMilliseconds(-500), range.Start);
  }

  [Fact]
  public void Parse_MicrosecondUnit_u()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 100u", Now, out var range));
    Assert.Equal(Now.AddTicks(-1000), range.Start); // 100 µs = 1000 ticks
  }

  // -----------------------------------------------------------------------
  // Range expressions (anchor..anchor)
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_SimpleRange_NowMinus1hToNow()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 1h..$now", Now, out var range));
    Assert.Equal(Now.AddHours(-1), range.Start);
    Assert.Equal(Now, range.End);
  }

  [Fact]
  public void Parse_Range_YesterdayToToday()
  {
    Assert.True(TickExpressionParser.TryParse("$yesterday..$today", Now, out var range));
    Assert.Equal(new DateTimeOffset(2025, 6, 14, 0, 0, 0, TimeSpan.Zero), range.Start);
    Assert.Equal(new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero), range.End);
  }

  [Fact]
  public void Parse_Range_IsoLiterals()
  {
    var expr = "2025-01-10T09:00:00..2025-01-10T17:00:00";
    Assert.True(TickExpressionParser.TryParse(expr, Now, out var range));
    Assert.Equal(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero), range.Start);
    Assert.Equal(new DateTimeOffset(2025, 1, 10, 17, 0, 0, TimeSpan.Zero), range.End);
  }

  [Fact]
  public void Parse_Range_MixedAnchors()
  {
    // Fixed start, relative end
    var expr = "2025-06-15T10:00:00..$now";
    Assert.True(TickExpressionParser.TryParse(expr, Now, out var range));
    Assert.Equal(new DateTimeOffset(2025, 6, 15, 10, 0, 0, TimeSpan.Zero), range.Start);
    Assert.Equal(Now, range.End);
  }

  [Fact]
  public void Parse_Range_BothAnchorsWithArithmetic()
  {
    var expr = "$now - 2h..$now - 30m";
    Assert.True(TickExpressionParser.TryParse(expr, Now, out var range));
    Assert.Equal(Now.AddHours(-2), range.Start);
    Assert.Equal(Now.AddMinutes(-30), range.End);
  }

  // -----------------------------------------------------------------------
  // Duration expressions (anchor;duration)
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_Duration_NowMinus1hSpan30m()
  {
    Assert.True(TickExpressionParser.TryParse("$now - 1h;30m", Now, out var range));
    Assert.Equal(Now.AddHours(-1), range.Start);
    Assert.Equal(Now.AddHours(-1).AddMinutes(30), range.End);
  }

  [Fact]
  public void Parse_Duration_IsoLiteralSpan2h()
  {
    Assert.True(TickExpressionParser.TryParse("2025-01-10T09:00:00;2h", Now, out var range));
    Assert.Equal(new DateTimeOffset(2025, 1, 10, 9, 0, 0, TimeSpan.Zero), range.Start);
    Assert.Equal(new DateTimeOffset(2025, 1, 10, 11, 0, 0, TimeSpan.Zero), range.End);
  }

  [Fact]
  public void Parse_Duration_CompoundDuration()
  {
    Assert.True(TickExpressionParser.TryParse("$today;1h30m", Now, out var range));
    var midnight = new DateTimeOffset(2025, 6, 15, 0, 0, 0, TimeSpan.Zero);
    Assert.Equal(midnight, range.Start);
    Assert.Equal(midnight.AddHours(1).AddMinutes(30), range.End);
  }

  // -----------------------------------------------------------------------
  // TryParseDuration (public helper)
  // -----------------------------------------------------------------------

  [Theory]
  [InlineData("1h", 60)]
  [InlineData("30m", 30)]
  [InlineData("1h30m", 90)]
  [InlineData("2d", 2 * 24 * 60)]
  [InlineData("500ms", 0)] // smaller than a minute
  public void TryParseDuration_ValidInputs(string input, int expectedMinutes)
  {
    Assert.True(TickExpressionParser.TryParseDuration(input, out var duration));
    if (expectedMinutes > 0)
      Assert.Equal(TimeSpan.FromMinutes(expectedMinutes), duration);
    else
      Assert.True(duration > TimeSpan.Zero);
  }

  [Theory]
  [InlineData("")]
  [InlineData("   ")]
  [InlineData("abc")]
  [InlineData("$now")]  // not a pure duration
  public void TryParseDuration_InvalidInputs(string input)
  {
    Assert.False(TickExpressionParser.TryParseDuration(input, out _));
  }

  // -----------------------------------------------------------------------
  // Edge cases / error handling
  // -----------------------------------------------------------------------

  [Fact]
  public void Parse_EmptyString_ReturnsFalse()
  {
    Assert.False(TickExpressionParser.TryParse("", Now, out _));
  }

  [Fact]
  public void Parse_NullString_ReturnsFalse()
  {
    Assert.False(TickExpressionParser.TryParse(null!, Now, out _));
  }

  [Fact]
  public void Parse_GarbageInput_ReturnsFalse()
  {
    Assert.False(TickExpressionParser.TryParse("not a tick expression", Now, out _));
  }

  [Fact]
  public void Parse_IncompleteRange_ReturnsFalse()
  {
    Assert.False(TickExpressionParser.TryParse("$now..", Now, out _));
  }

  [Fact]
  public void Parse_VariablesCaseInsensitive()
  {
    Assert.True(TickExpressionParser.TryParse("$NOW - 5m..$NOW", Now, out var range));
    Assert.Equal(Now.AddMinutes(-5), range.Start);
    Assert.Equal(Now, range.End);
  }

  // -----------------------------------------------------------------------
  // FormatTimestamp
  // -----------------------------------------------------------------------

  [Fact]
  public void FormatTimestamp_ProducesDuckDbCompatibleFormat()
  {
    var ts = new DateTimeOffset(2025, 1, 10, 9, 30, 15, 123, TimeSpan.Zero);
    var result = TickExpressionParser.FormatTimestamp(ts);
    Assert.Equal("2025-01-10 09:30:15.123000", result);
  }
}
