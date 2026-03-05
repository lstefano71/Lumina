using DotNextLock = DotNext.Threading.AsyncReaderWriterLock;

namespace Lumina.Core.Concurrency;

/// <summary>
/// A lightweight async-compatible reader/writer lock.
/// <para>
/// Delegates to <c>DotNext.Threading.AsyncReaderWriterLock</c>
/// (<c>DotNext.Threading</c> NuGet package), a high-performance
/// queue-based async reader/writer lock.
/// </para>
/// <para>
/// Usage pattern:
/// <code>
///   await using var guard = await _lock.ReaderLockAsync(ct);
///   // ... read operations ...
/// </code>
/// </para>
/// </summary>
public sealed class AsyncReaderWriterLock : IDisposable
{
  private readonly DotNextLock _inner = new();

  /// <summary>
  /// Acquires a reader lock. Multiple readers can hold the lock simultaneously.
  /// Returns an <see cref="IAsyncDisposable"/> guard that releases the lock on dispose.
  /// </summary>
  public async Task<IAsyncDisposable> ReaderLockAsync(CancellationToken cancellationToken = default)
  {
    await _inner.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
    return new LockGuard(_inner);
  }

  /// <summary>
  /// Acquires a writer lock. Only one writer can hold the lock, and it
  /// blocks until all readers have released.
  /// Returns an <see cref="IAsyncDisposable"/> guard that releases the lock on dispose.
  /// </summary>
  public async Task<IAsyncDisposable> WriterLockAsync(CancellationToken cancellationToken = default)
  {
    await _inner.EnterWriteLockAsync(cancellationToken).ConfigureAwait(false);
    return new LockGuard(_inner);
  }

  /// <inheritdoc />
  public void Dispose() => _inner.Dispose();

  // -----------------------------------------------------------------------
  //  Guard type (IAsyncDisposable) — DotNext uses Release() for both modes
  // -----------------------------------------------------------------------

  private sealed class LockGuard(DotNextLock inner) : IAsyncDisposable
  {
    private int _disposed;

    public ValueTask DisposeAsync()
    {
      if (Interlocked.Exchange(ref _disposed, 1) == 0) {
        inner.Release();
      }
      return ValueTask.CompletedTask;
    }
  }
}
