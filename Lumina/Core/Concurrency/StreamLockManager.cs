using System.Collections.Concurrent;

namespace Lumina.Core.Concurrency;

/// <summary>
/// Provides per-stream <see cref="AsyncReaderWriterLock"/> instances.
/// <para>
/// <b>Readers</b> (SQL queries) call <see cref="AcquireStreamReaderAsync"/> so
/// that multiple queries on the same stream can execute in parallel.
/// </para>
/// <para>
/// <b>Writers</b> (compaction file-swap + delete) call
/// <see cref="AcquireStreamWriterAsync"/> which blocks until all active
/// readers for that stream have completed.
/// </para>
/// Registered as a singleton in DI.
/// </summary>
public sealed class StreamLockManager
{
  private readonly ConcurrentDictionary<string, AsyncReaderWriterLock> _locks = new(StringComparer.OrdinalIgnoreCase);

  /// <summary>
  /// Global reader/writer lock that protects the query↔compaction boundary.
  /// <list type="bullet">
  ///   <item><b>Readers</b> (all DuckDB query methods) hold a reader lock so
  ///         that file deletions cannot happen while a query is executing.</item>
  ///   <item><b>Writer</b> (compaction file-swap phase) holds a writer lock
  ///         while deleting old Parquet files and refreshing DuckDB views.</item>
  /// </list>
  /// </summary>
  public AsyncReaderWriterLock CompactionLock { get; } = new();

  /// <summary>
  /// Gets (or creates) the <see cref="AsyncReaderWriterLock"/> for the given stream.
  /// </summary>
  public AsyncReaderWriterLock GetLock(string stream)
    => _locks.GetOrAdd(stream, _ => new AsyncReaderWriterLock());

  /// <summary>
  /// Acquires a reader lock for the given stream.
  /// The returned guard must be disposed when the read operation is complete.
  /// </summary>
  public Task<IAsyncDisposable> AcquireStreamReaderAsync(string stream, CancellationToken cancellationToken = default)
    => GetLock(stream).ReaderLockAsync(cancellationToken);

  /// <summary>
  /// Acquires a writer lock for the given stream.
  /// The returned guard must be disposed when the write (file-swap/delete) operation is complete.
  /// </summary>
  public Task<IAsyncDisposable> AcquireStreamWriterAsync(string stream, CancellationToken cancellationToken = default)
    => GetLock(stream).WriterLockAsync(cancellationToken);
}
