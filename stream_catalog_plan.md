# Plan: Stream Catalog for Atomic Visibility

## Objective

Introduce a Stream Catalog (`catalog.json`) to serve as the single source-of-truth for active data files in Lumina. This eliminates read-during-write corruption, prevents duplicate row visibility windows during L2 compaction, and provides ACID-like atomic transitions.

## 1. Catalog Models & Management

* **Directory:** Create a new folder `Lumina/Storage/Catalog/`.
* **Models:**
  * `CatalogEntry`: Represents an active file (Stream Name, Date, File Path, Level `L1`/`L2`, Row Count).
  * `StreamCatalog`: Represents the full state (a list or dictionary of `CatalogEntry` items).
* **`CatalogManager`:** Responsible for maintaining the in-memory catalog state and safely persisting it to disk.

## 2. Safe-Write Pattern (Corruption Prevention)

To prevent catalog corruption during process crashes or power loss:

* Updates never overwrite `catalog.json` directly.
* Serialize the new state to `catalog.tmp.json` using a `FileStream`.
* Call `FileStream.Flush(default)` (or `Flush(true)` if available to bypass OS caches).
* Atomically rename the file using `File.Move("catalog.tmp.json", "catalog.json", overwrite: true)`. At the OS level, this metadata pointer update is atomic.

## 3. Disaster Recovery (Auto-Rebuilder)

If `catalog.json` is deleted, severely corrupted, or unavailable on startup, the system must self-heal: Add a CatalogRebuilder.RecoverFromDiskAsync() method that runs on startup if the catalog fails to parse.

* **Rebuild Logic:** Scan the physical `L1` and `L2` directories.
* **Conflict Resolution:** Group files by `StreamName` and `Date`.
  * If an `L2` file exists for a specific Stream+Date, inherently trust it and add it to the rebuilt catalog. Ignore any overlapping `L1` files.
  * If no `L2` file exists, add all `L1` files for that Stream+Date.
* Save this deterministically rebuilt state as the new `catalog.json`.

## 4. Integration with Write Paths

* **L1 Writes (Ingestion):** Once a new L1 Parquet file is sealed and completely written to disk, invoke `CatalogManager.AddFileAsync(...)` to add it to the catalog and trigger a Safe-Write.
* **L2 Compaction (The Atomic Commit):**
    1. Write the new consolidated `L2.parquet` file to disk.
    2. Invoke `CatalogManager.ReplaceFilesAsync(oldL1Files, newL2File)`. This removes the L1 files and adds the L2 file in memory, then performs a Safe-Write to disk. **This is the atomic commit point.** Queries immediately following this will read exclusively from the L2 file.
    3. Physically delete the old `oldL1Files` from the directory. (If a crash happens here, they become unreferenced orphans, invisible to queries. Startup GC will clean them up).

## 5. View Creation & File Discovery

* **Refactor `ParquetManager`:** Modify `GetStreamFiles()`, `GetL1Files()`, and `GetTotalSize()` to query the `CatalogManager`'s in-memory state instead of using `Directory.GetFiles(...)`.
* **DuckDbQueryService:** Naturally inherits this change. DuckDB will now build its views dynamically based *only* on the files explicitly registered in the catalog.

## 6. Startup Garbage Collection

* During application startup, after initializing the `CatalogManager`, run a background GC task.
* Compare the physical files on disk against the active files in the catalog.
* Delete any `.parquet` files that are entirely unreferenced (e.g., L1 files left over from a mid-compaction crash, or `.tmp` files from interrupted writes).

## 7. Verification & Testing

* **Safe-Write Test:** Unit test simulating an exception exactly during the `File.Move` operation to ensure the original `catalog.json` remains untouched.
* **Recovery Test:** Integration test that overwrites `catalog.json` with invalid JSON and deposits overlapping L1/L2 files, asserting that the subsystem successfully rebuilds the accurate state.
* **Concurrency Test:** Run background read loops checking for row duplications while forcing L2 compactions.
