## Plan: Metadata-Driven Compaction & System Fields

**Steps**

1. **Phase 1: System Field Standardization (`timestamp` -> `_t`)**
   - Refactor ingestion components (e.g., `JsonNormalizer.cs`, `OtlpNormalizer.cs`) to map the incoming event time to `_t` instead of `timestamp`.
   - Update `LogEntry.cs` (or equivalent models) to reflect the new property name.
   - Update `SqlValidator.cs` and `DuckDbQueryService.cs` so queries naturally expect and order by `_t`.

2. **Phase 2: Elevate `CatalogEntry` Metadata**
   - Update `CatalogEntry.cs`. Remove the single `Date` field.
   - Add `MinTime` (or `StartTime`) and `MaxTime` (or `EndTime`) as `DateTime` (or long/ticks) properties.
   - Update `CatalogManager.cs` to persist and load these new fields. 

3. **Phase 3: Robust `CatalogRebuilder` & `ParquetManager`**
   - Remove all filename `Split` and `Parse` logic from `ParquetManager` and `CatalogRebuilder`.
   - When `CatalogRebuilder.cs` scans orphans, it will open the file using your existing Parquet library (e.g., Parquet.Net), read the file's footer/metadata, and extract the Min/Max statistics for the `_t` column. This is extremely fast as it doesn't require reading the actual rows.
   - `ParquetManager.GetFilesInRange` changes to purely query the `CatalogManager`: `WHERE MinTime <= queryEnd AND MaxTime >= queryStart`.

4. **Phase 4: N-Tier Compaction Implementation**
   - Now that filenames are irrelevant to the engine, implement `L2Compactor` to run daily and monthly logic purely via Catalog queries.
   - **Daily L1 -> L2**: Query catalog for `L1` files where `MaxTime < UtcNow.Date`. Group by `MinTime.Date`. Compact into `[stream]_[yyyyMMdd].parquet` (the name is now just a helpful human convention, not parsed by code), and record the new exact `MinTime`/`MaxTime` in the catalog entry.
   - **Monthly L2 -> L2**: Query catalog for `L2` files where `MinTime` and `MaxTime` fall within a completed month. Compact into `[stream]_[yyyyMM].parquet`.

**Verification**
- Run existing ingestion/query tests after renaming to `_t`.
- Delete the `catalog.json` mid-test, trigger the Rebuilder, and verify it correctly extracts `_t` min/max stats from the `.parquet` files and restores exact query routing.
- Execute the L2 compactor and verify that daily and monthly files are generated and properly tracked in the catalog.

**Decisions**
- Chose to extract `_t` bounds from Parquet metadata block statistics during rebuilds. This avoids O(N) full file scans and makes rebuilds nearly instantaneous while remaining perfectly accurate.
- Retained human-readable suffixes (`yyyyMMdd`) in the physical file names for operational debugging, but stripped all application logic dependency from them.
