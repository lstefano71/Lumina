 
# Lumina — Copilot Instructions (Concise)

Purpose: short, essential rules for contributors and coding agents working on Lumina.

Quick rules
- **System field names:** use underscore-prefixed names: `_s`, `_t`, `_l`, `_m`, `_traceid`, `_spanid`, `_duration_ms`.
- **Required fields:** `_s`, `_t`, `_m` must be present; others are optional.
- **No fallback:** do not implement backward-compatibility for legacy names (e.g., `timestamp`, `stream`). If a required underscore field is missing, treat it as missing.

JSON ingestion
- Endpoints accept payloads that use the underscore field names. Extra keys are user attributes (promote to Parquet columns when frequent, else store in `_meta`).

Parquet / DuckDB
- Base columns (in every Parquet file / hot-table): `_s`, `_t`, `_l`, `_m`, `_traceid`, `_spanid`, `_duration_ms`.
- Helpers that check fixed columns must match these exact names.

C# conventions
- Domain models use PascalCase. API DTOs map underscore names via `[JsonPropertyName]`.

Queries and scripts
- In SQL, reference system fields by underscore names. Query helpers rely on these exact names.
- PowerShell scripts and utilities use underscore fields in payloads and result access.

Project layout (short)
- `Lumina/Core`, `Lumina/Ingestion`, `Lumina/Storage`, `Lumina/Query`, `Tests` — see repo for details.

If you want this shortened further or prefer a version tailored to a specific audience (developers, ops, or agents), tell me which target and I will adjust.
