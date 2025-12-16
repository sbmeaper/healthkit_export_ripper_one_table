# HealthKit Export Parser - Project Status

## Purpose
Parse Apple Health `export.xml` into a queryable format for natural language queries via MCP server.

## Key Design Decisions

**Single flat table schema** — Optimizes for LLM SQL generation (fewer tables = fewer wrong choices).

```sql
CREATE TABLE health (
    type VARCHAR,           -- e.g., 'HeartRate', 'StepCount', 'WorkoutRunning'
    value DOUBLE,
    unit VARCHAR,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    duration_min DOUBLE,    -- workout only
    distance_km DOUBLE,     -- workout only
    energy_kcal DOUBLE,     -- workout only
    source_name VARCHAR
)
```

**Type normalization** — Apple's verbose prefixes stripped for cleaner queries:
- `HKQuantityTypeIdentifierHeartRate` → `HeartRate`
- `HKWorkoutActivityTypeRunning` → `WorkoutRunning`

**Parquet over DuckDB native format** — Decided during this session:
- Simpler (just a file)
- Portable (other tools can read it)
- Better compression
- DuckDB queries Parquet directly: `SELECT * FROM 'health.parquet'`

## Files

| File | Purpose |
|------|---------|
| `healthkit_duckdb.py` | Writes to DuckDB native format (slower) |
| `healthkit_to_parquet.py` | Writes to Parquet (faster, preferred) |
| `export.xml` | Source data from Apple Health (2.65GB) |
| `health.db` | Output from DuckDB version (complete) |
| `health.parquet` | Output from Parquet version (in progress) |

## Performance Comparison

| Version | Rows/sec | Total time (5.6M rows) |
|---------|----------|------------------------|
| DuckDB | ~1,675 | 56 min |
| Parquet | ~2,700 | ~35 min (estimated) |

Bottleneck is disk I/O, not CPU or XML parsing.

## Current State

- ✅ Python 3.13 + duckdb installed in `.venv`
- ✅ DuckDB version complete: 5,628,491 records in 3,361 seconds
- 🔄 Parquet version running (was at 1.2M rows when session ended)
- Temp chunk files (`.chunk_*.parquet`) will be merged and deleted on completion

## Next Steps

1. Verify `health.parquet` completed successfully
2. Clean up temp chunk files if script crashed
3. Build MCP server to query the Parquet file
4. Test natural language → SQL generation

## Environment

- macOS Tahoe
- PyCharm project: `healthkit_export_ripper_one_table`
- Python 3.13 via `/usr/local/bin/python3.13`
- Virtual env: `.venv` in project root

## Usage

```bash
# Activate venv (if not automatic in PyCharm terminal)
source .venv/bin/activate

# Run parser
python healthkit_to_parquet.py export.xml -o health.parquet

# Query result with DuckDB CLI
duckdb -c "SELECT type, COUNT(*) FROM 'health.parquet' GROUP BY type ORDER BY 2 DESC"
```