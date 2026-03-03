using FluentAssertions;

using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class Crc8Tests
{
  [Fact]
  public void Compute_EmptyInput_ShouldReturnZero()
  {
    var result = Crc8.Compute(ReadOnlySpan<byte>.Empty);

    result.Should().Be(0);
  }

  [Fact]
  public void Compute_SingleByte_ShouldReturnDeterministicValue()
  {
    var data = new byte[] { 0x42 };

    var result = Crc8.Compute(data);

    result.Should().NotBe(0);
  }

  [Fact]
  public void Compute_SameInput_ShouldReturnSameResult()
  {
    var data = new byte[] { 0x01, 0x02, 0x03, 0x04 };

    var first = Crc8.Compute(data);
    var second = Crc8.Compute(data);

    first.Should().Be(second);
  }

  [Fact]
  public void Compute_DifferentInput_ShouldReturnDifferentResults()
  {
    var data1 = new byte[] { 0x01, 0x02, 0x03, 0x04 };
    var data2 = new byte[] { 0x04, 0x03, 0x02, 0x01 };

    var result1 = Crc8.Compute(data1);
    var result2 = Crc8.Compute(data2);

    result1.Should().NotBe(result2);
  }

  [Fact]
  public void Compute_WithInitial_ShouldChainCorrectly()
  {
    var part1 = new byte[] { 0x01, 0x02 };
    var part2 = new byte[] { 0x03, 0x04 };
    var full = new byte[] { 0x01, 0x02, 0x03, 0x04 };

    var chainedCrc = Crc8.Compute(part2, Crc8.Compute(part1));
    var fullCrc = Crc8.Compute(full);

    chainedCrc.Should().Be(fullCrc);
  }

  [Fact]
  public void Validate_CorrectCrc_ShouldReturnTrue()
  {
    var data = new byte[] { 0x10, 0x20, 0x30, 0x40 };
    var crc = Crc8.Compute(data);

    var result = Crc8.Validate(data, crc);

    result.Should().BeTrue();
  }

  [Fact]
  public void Validate_IncorrectCrc_ShouldReturnFalse()
  {
    var data = new byte[] { 0x10, 0x20, 0x30, 0x40 };
    var wrongCrc = (byte)(Crc8.Compute(data) ^ 0xFF);

    var result = Crc8.Validate(data, wrongCrc);

    result.Should().BeFalse();
  }

  [Fact]
  public void Validate_CorruptedData_ShouldReturnFalse()
  {
    var data = new byte[] { 0x10, 0x20, 0x30, 0x40 };
    var crc = Crc8.Compute(data);

    // Flip a single bit
    data[2] ^= 0x01;

    var result = Crc8.Validate(data, crc);

    result.Should().BeFalse();
  }

  [Fact]
  public void Compute_LargeData_ShouldReturnConsistentResult()
  {
    var data = new byte[1024];
    new Random(42).NextBytes(data);

    var result1 = Crc8.Compute(data);
    var result2 = Crc8.Compute(data);

    result1.Should().Be(result2);
  }

  [Theory]
  [InlineData(new byte[] { 0x00 })]
  [InlineData(new byte[] { 0xFF })]
  [InlineData(new byte[] { 0x00, 0x00, 0x00, 0x00 })]
  [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF })]
  public void Compute_BoundaryValues_ShouldNotThrow(byte[] data)
  {
    var act = () => Crc8.Compute(data);

    act.Should().NotThrow();
  }
}
