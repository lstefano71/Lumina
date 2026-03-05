namespace Lumina.Core.Concurrency;

/// <summary>
/// A lightweight async-compatible reader/writer lock.
/// Multiple readers can hold the lock concurrently; a single writer blocks
/// all readers and other writers.
/// <para>
/// Usage pattern:
/// <code>
///   await using var guard = await _lock.ReaderLockAsync(ct);
///   // ... read operations ...
/// </code>
/// </para>
/// </summary>
public sealed class AsyncReaderWriterLock
{
  // Writers wait on this gate (only 1 writer at a time)
  private readonly SemaphoreSlim _writerGate = new(1, 1);

  // Readers increment/decrement _readerCount under _readerCountLock.
  // The first reader blocks writers; the last reader unblocks them.
  private readonly SemaphoreSlim _readerCountLock = new(1, 1);
  private int _readerCount;

  /// <summary>
  /// Acquires a reader lock. Multiple readers can hold the lock simultaneously.
  /// Returns an <see cref="IAsyncDisposable"/> guard that releases the lock on dispose.
  /// </summary>
  public async Task<IAsyncDisposable> ReaderLockAsync(CancellationToken cancellationToken = default)
  {
    await _readerCountLock.WaitAsync(cancellationToken).ConfigureAwait(false);
    try {
      _readerCount++;
      if (_readerCount == 1) {
        // First reader blocks the writer
        await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
      }
    } finally {
      _readerCountLock.Release();
    }

    return new ReaderGuard(this);
  }

  /// <summary>
  /// Acquires a writer lock. Only one writer can hold the lock, and it
  /// blocks until all readers have released.
  /// Returns an <see cref="IAsyncDisposable"/> guard that releases the lock on dispose.
  /// </summary>
  public async Task<IAsyncDisposable> WriterLockAsync(CancellationToken cancellationToken = default)
  {
    await _writerGate.WaitAsync(cancellationToken).ConfigureAwait(false);
    return new WriterGuard(this);
  }

  private async Task ReleaseReaderAsync()
  {
    await _readerCountLock.WaitAsync().ConfigureAwait(false);
    try {
      _readerCount--;
      if (_readerCount == 0) {
        // Last reader unblocks the writer
        _writerGate.Release();
      }
    } finally {
      _readerCountLock.Release();
    }
  }

  private void ReleaseWriter()
  {
    _writerGate.Release();
  }

  // -----------------------------------------------------------------------
  //  Guard types (IAsyncDisposable)
  // -----------------------------------------------------------------------

  private sealed class ReaderGuard : IAsyncDisposable
  {
    private readonly AsyncReaderWriterLock _owner;
    private int _disposed;

    public ReaderGuard(AsyncReaderWriterLock owner) => _owner = owner;

    public async ValueTask DisposeAsync()
    {
      if (Interlocked.Exchange(ref _disposed, 1) == 0) {
        await _owner.ReleaseReaderAsync().ConfigureAwait(false);
      }
    }
  }

  private sealed class WriterGuard : IAsyncDisposable
  {
    private readonly AsyncReaderWriterLock _owner;
    private int _disposed;

    public WriterGuard(AsyncReaderWriterLock owner) => _owner = owner;

    public ValueTask DisposeAsync()
    {
      if (Interlocked.Exchange(ref _disposed, 1) == 0) {
        _owner.ReleaseWriter();
      }
      return ValueTask.CompletedTask;
    }
  }
}
