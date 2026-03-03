using FluentAssertions;

using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class WalFormatTests
{
  // --- WalFormat constants ---

  [Fact]
  public void WalFormat_Magic_ShouldBeKnownConstant()
  {
    // "LUMI" in little-endian
    WalFormat.Magic.Should().Be(0x494D554C);
  }

  [Fact]
  public void WalFormat_SyncMarker_ShouldBeKnownConstant()
  {
    WalFormat.SyncMarker.Should().Be(0x0CB0CEFA);
  }

  [Fact]
  public void WalFormat_SyncMarkerBytes_ShouldMatchConstant()
  {
    var bytes = WalFormat.SyncMarkerBytes;
    bytes.Length.Should().Be(4);
    bytes[0].Should().Be(0xFA);
    bytes[1].Should().Be(0xCE);
    bytes[2].Should().Be(0xB0);
    bytes[3].Should().Be(0x0C);
  }

  [Fact]
  public void WalFormat_CurrentVersion_ShouldBeOne()
  {
    WalFormat.CurrentVersion.Should().Be(0x01);
  }

  // --- WalEntryType ---

  [Theory]
  [InlineData(WalEntryType.StandardLog, 0x01)]
  [InlineData(WalEntryType.Metric, 0x02)]
  [InlineData(WalEntryType.Trace, 0x03)]
  public void WalEntryType_ShouldHaveCorrectValues(WalEntryType type, byte expected)
  {
    ((byte)type).Should().Be(expected);
  }

  // --- WalFileHeader ---

  [Fact]
  public void WalFileHeader_Size_ShouldBe8Bytes()
  {
    WalFileHeader.Size.Should().Be(8);
  }

  [Fact]
  public void WalFileHeader_CreateDefault_ShouldBeValid()
  {
    var header = WalFileHeader.CreateDefault();

    header.IsValid.Should().BeTrue();
    header.Magic.Should().Be(WalFileHeader.ExpectedMagic);
    header.Version.Should().Be(WalFormat.CurrentVersion);
    header.Flags.Should().Be(0);
  }

  [Fact]
  public void WalFileHeader_WriteTo_ReadFrom_ShouldRoundTrip()
  {
    var original = WalFileHeader.CreateDefault();
    var buffer = new byte[WalFileHeader.Size];

    original.WriteTo(buffer);
    var restored = WalFileHeader.ReadFrom(buffer);

    restored.Magic.Should().Be(original.Magic);
    restored.Version.Should().Be(original.Version);
    restored.Flags.Should().Be(original.Flags);
    restored.IsValid.Should().BeTrue();
  }

  [Fact]
  public void WalFileHeader_WriteTo_ShouldProduceLittleEndianMagic()
  {
    var header = WalFileHeader.CreateDefault();
    var buffer = new byte[WalFileHeader.Size];

    header.WriteTo(buffer);

    // "LUMI" in ASCII / little-endian
    buffer[0].Should().Be(0x4C); // 'L'
    buffer[1].Should().Be(0x55); // 'U'
    buffer[2].Should().Be(0x4D); // 'M'
    buffer[3].Should().Be(0x49); // 'I'
  }

  [Fact]
  public void WalFileHeader_WithCustomVersion_ShouldNotBeValid()
  {
    var header = new WalFileHeader(version: 0x99);

    header.Magic.Should().Be(WalFileHeader.ExpectedMagic);
    header.Version.Should().Be(0x99);
    header.IsValid.Should().BeFalse();
  }

  [Fact]
  public void WalFileHeader_WriteTo_BufferTooSmall_ShouldThrow()
  {
    var header = WalFileHeader.CreateDefault();
    var smallBuffer = new byte[4];

    var act = () => header.WriteTo(smallBuffer);

    act.Should().Throw<ArgumentOutOfRangeException>();
  }

  [Fact]
  public void WalFileHeader_ReadFrom_BufferTooSmall_ShouldThrow()
  {
    var smallBuffer = new byte[4];

    var act = () => WalFileHeader.ReadFrom(smallBuffer);

    act.Should().Throw<ArgumentOutOfRangeException>();
  }

  // --- WalFrameHeader ---

  [Fact]
  public void WalFrameHeader_Size_ShouldBe14Bytes()
  {
    WalFrameHeader.Size.Should().Be(14);
  }

  [Fact]
  public void WalFrameHeader_Constructor_ShouldSetSyncMarkerAndCrc()
  {
    var header = new WalFrameHeader(256, WalEntryType.StandardLog);

    header.SyncMarker.Should().Be(WalFormat.SyncMarker);
    header.Length.Should().Be(256);
    header.InvertedLength.Should().Be(~(uint)256);
    header.Type.Should().Be(WalEntryType.StandardLog);
    header.IsValid.Should().BeTrue();
  }

  [Fact]
  public void WalFrameHeader_WriteTo_ReadFrom_ShouldRoundTrip()
  {
    var original = new WalFrameHeader(512, WalEntryType.Trace);
    var buffer = new byte[WalFrameHeader.Size];

    original.WriteTo(buffer);
    var restored = WalFrameHeader.ReadFrom(buffer);

    restored.SyncMarker.Should().Be(original.SyncMarker);
    restored.Length.Should().Be(original.Length);
    restored.InvertedLength.Should().Be(original.InvertedLength);
    restored.Type.Should().Be(original.Type);
    restored.HeaderCrc.Should().Be(original.HeaderCrc);
    restored.IsValid.Should().BeTrue();
  }

  [Fact]
  public void WalFrameHeader_IsValid_FalseWhenLengthTampered()
  {
    var header = new WalFrameHeader(100, WalEntryType.Metric);
    var buffer = new byte[WalFrameHeader.Size];
    header.WriteTo(buffer);

    // Corrupt the Length field (bytes 4-7)
    buffer[4] ^= 0xFF;

    var corrupted = WalFrameHeader.ReadFrom(buffer);
    corrupted.IsValid.Should().BeFalse();
  }

  [Fact]
  public void WalFrameHeader_IsValid_FalseWhenSyncMarkerWrong()
  {
    var header = new WalFrameHeader(100, WalEntryType.StandardLog);
    var buffer = new byte[WalFrameHeader.Size];
    header.WriteTo(buffer);

    // Corrupt the sync marker (first byte)
    buffer[0] = 0x00;

    var corrupted = WalFrameHeader.ReadFrom(buffer);
    corrupted.IsValid.Should().BeFalse();
  }

  [Fact]
  public void WalFrameHeader_IsValid_FalseWhenCrcCorrupted()
  {
    var header = new WalFrameHeader(100, WalEntryType.StandardLog);
    var buffer = new byte[WalFrameHeader.Size];
    header.WriteTo(buffer);

    // Corrupt the CRC byte (last byte)
    buffer[WalFrameHeader.Size - 1] ^= 0xFF;

    var corrupted = WalFrameHeader.ReadFrom(buffer);
    corrupted.IsValid.Should().BeFalse();
  }

  [Fact]
  public void WalFrameHeader_TryValidate_TrueForValid()
  {
    var header = new WalFrameHeader(64, WalEntryType.StandardLog);
    var buffer = new byte[WalFrameHeader.Size];
    header.WriteTo(buffer);

    var result = WalFrameHeader.TryValidate(buffer, out var parsed);

    result.Should().BeTrue();
    parsed.Length.Should().Be(64);
  }

  [Fact]
  public void WalFrameHeader_TryValidate_FalseForSmallBuffer()
  {
    var result = WalFrameHeader.TryValidate(new byte[3], out _);

    result.Should().BeFalse();
  }

  [Fact]
  public void WalFrameHeader_EndOffset_CalculatesCorrectly()
  {
    var header = new WalFrameHeader(200, WalEntryType.StandardLog);

    header.EndOffset.Should().Be(WalFrameHeader.Size + 200);
  }

  [Theory]
  [InlineData(0u)]
  [InlineData(1u)]
  [InlineData(uint.MaxValue / 2)]
  public void WalFrameHeader_InvertedLength_AlwaysMatchesBitwiseNot(uint length)
  {
    var header = new WalFrameHeader(length, WalEntryType.StandardLog);

    header.InvertedLength.Should().Be(~length);
    header.IsValid.Should().BeTrue();
  }

  [Fact]
  public void WalFrameHeader_WriteTo_BufferTooSmall_ShouldThrow()
  {
    var header = new WalFrameHeader(10, WalEntryType.StandardLog);
    var act = () => header.WriteTo(new byte[5]);

    act.Should().Throw<ArgumentOutOfRangeException>();
  }

  [Theory]
  [InlineData(WalEntryType.StandardLog)]
  [InlineData(WalEntryType.Metric)]
  [InlineData(WalEntryType.Trace)]
  public void WalFrameHeader_AllEntryTypes_ShouldBeValid(WalEntryType type)
  {
    var header = new WalFrameHeader(128, type);

    header.IsValid.Should().BeTrue();
    header.Type.Should().Be(type);
  }
}
