using FluentAssertions;

using Lumina.Storage.Compaction;

using System.Text;

using Xunit;

namespace Lumina.Tests.Storage;

public class CursorFileHeaderTests
{
  [Fact]
  public void Header_ShouldHaveCorrectSize()
  {
    CursorFileHeader.Size.Should().Be(16);
  }

  [Fact]
  public void Header_ShouldHaveCorrectMagic()
  {
    CursorFileHeader.ExpectedMagic.Should().Be(0x52434C4C);
  }

  [Fact]
  public void Header_ShouldHaveCorrectVersion()
  {
    CursorFileHeader.CurrentVersion.Should().Be(0x01);
  }

  [Fact]
  public void WriteTo_ShouldWriteCorrectBytes()
  {
    var header = new CursorFileHeader(0x12345678, 100);
    Span<byte> buffer = stackalloc byte[CursorFileHeader.Size];
    header.WriteTo(buffer);

    // Check magic (little-endian)
    BitConverter.ToUInt32(buffer.Slice(0, 4)).Should().Be(CursorFileHeader.ExpectedMagic);

    // Check version
    buffer[4].Should().Be(CursorFileHeader.CurrentVersion);

    // Check reserved bytes
    buffer[5].Should().Be(0);
    buffer[6].Should().Be(0);
    buffer[7].Should().Be(0);

    // Check checksum (little-endian)
    BitConverter.ToUInt32(buffer.Slice(8, 4)).Should().Be(0x12345678);

    // Check length (little-endian)
    BitConverter.ToUInt32(buffer.Slice(12, 4)).Should().Be(100);
  }

  [Fact]
  public void ReadFrom_ShouldRoundTrip()
  {
    var original = new CursorFileHeader(0xDEADBEEF, 12345);
    Span<byte> buffer = stackalloc byte[CursorFileHeader.Size];
    original.WriteTo(buffer);

    var read = CursorFileHeader.ReadFrom(buffer);

    read.Magic.Should().Be(original.Magic);
    read.Version.Should().Be(original.Version);
    read.PayloadChecksum.Should().Be(original.PayloadChecksum);
    read.PayloadLength.Should().Be(original.PayloadLength);
  }

  [Fact]
  public void HasValidMagic_ShouldReturnTrueForValidMagic()
  {
    var header = new CursorFileHeader(0, 0);
    header.HasValidMagic.Should().BeTrue();
  }

  [Fact]
  public void HasSupportedVersion_ShouldReturnTrueForCurrentVersion()
  {
    var header = new CursorFileHeader(0, 0);
    header.HasSupportedVersion.Should().BeTrue();
  }

  [Fact]
  public void IsValid_ShouldReturnTrueForValidHeader()
  {
    var header = new CursorFileHeader(0, 0);
    header.IsValid.Should().BeTrue();
  }

  [Fact]
  public void CreateForPayload_ShouldComputeCorrectChecksum()
  {
    var payload = Encoding.UTF8.GetBytes("{\"test\":\"data\"}");
    var header = CursorFileHeader.CreateForPayload(payload);

    header.PayloadLength.Should().Be((uint)payload.Length);
    header.PayloadChecksum.Should().NotBe(0);
  }

  [Fact]
  public void ValidatePayload_ShouldReturnTrueForValidPayload()
  {
    var payload = Encoding.UTF8.GetBytes("{\"test\":\"data\"}");
    var header = CursorFileHeader.CreateForPayload(payload);

    header.ValidatePayload(payload).Should().BeTrue();
  }

  [Fact]
  public void ValidatePayload_ShouldReturnFalseForCorruptedPayload()
  {
    var payload = Encoding.UTF8.GetBytes("{\"test\":\"data\"}");
    var header = CursorFileHeader.CreateForPayload(payload);

    var corrupted = Encoding.UTF8.GetBytes("{\"test\":\"corrupted\"}");
    header.ValidatePayload(corrupted).Should().BeFalse();
  }

  [Fact]
  public void ValidatePayload_ShouldReturnFalseForWrongLength()
  {
    var payload = Encoding.UTF8.GetBytes("{\"test\":\"data\"}");
    var header = CursorFileHeader.CreateForPayload(payload);

    var shorter = Encoding.UTF8.GetBytes("{\"test\"");
    header.ValidatePayload(shorter).Should().BeFalse();
  }

  [Fact]
  public void TryRead_ShouldReturnTrueForValidBuffer()
  {
    var header = new CursorFileHeader(0x12345678, 100);
    Span<byte> buffer = stackalloc byte[CursorFileHeader.Size];
    header.WriteTo(buffer);

    var result = CursorFileHeader.TryRead(buffer, out var read);

    result.Should().BeTrue();
    read.Magic.Should().Be(CursorFileHeader.ExpectedMagic);
  }

  [Fact]
  public void TryRead_ShouldReturnFalseForSmallBuffer()
  {
    Span<byte> buffer = stackalloc byte[CursorFileHeader.Size - 1];

    var result = CursorFileHeader.TryRead(buffer, out var read);

    result.Should().BeFalse();
  }
}