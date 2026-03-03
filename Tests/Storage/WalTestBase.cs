using Lumina.Core.Configuration;

namespace Lumina.Tests.Storage;

/// <summary>
/// Base class for WAL tests providing temporary directory management.
/// </summary>
public abstract class WalTestBase : IDisposable
{
    protected readonly string TempDirectory;
    
    protected WalTestBase()
    {
        TempDirectory = Path.Combine(Path.GetTempPath(), "LuminaTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(TempDirectory);
    }
    
    public void Dispose()
    {
        if (Directory.Exists(TempDirectory))
        {
            try
            {
                Directory.Delete(TempDirectory, recursive: true);
            }
            catch
            {
                // Ignore cleanup failures
            }
        }
        
        GC.SuppressFinalize(this);
    }
    
    protected string GetWalPath(string stream) =>
        Path.Combine(TempDirectory, stream, $"{DateTime.UtcNow:yyyyMMdd_HHmmss}_0000.wal");
    
    protected WalSettings GetTestSettings() => new()
    {
        DataDirectory = TempDirectory,
        MaxWalSizeBytes = 1024 * 1024, // 1 MB
        EnableWriteThrough = false,
        FlushIntervalMs = 100
    };
}