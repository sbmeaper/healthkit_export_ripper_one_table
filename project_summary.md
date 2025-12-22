# HealthKit MCP Project — Summary

## Status
**Debugging GPS extraction** (Dec 22, 2025)

Parquet export runs successfully but GPS coordinates are not being linked to workouts.

## Problem to Solve
- Export found 2,367 GPX files in `workout-routes/` folder
- Export produced 3,361 workout rows
- **0 workouts have GPS coordinates**

The `get_workout_route_path()` function isn't finding the `WorkoutRoute/FileReference` elements, or the path format doesn't match what we expect.

## Next Steps
1. Inspect a sample `Workout` element in `export.xml` to see actual structure
2. Check if `WorkoutRoute` and `FileReference` tags exist and what they look like
3. Fix the path matching logic in `healthkit_to_parquet.py`

## What Was Done This Session
1. Analyzed both export scripts (`healthkit_to_parquet.py` and `healthkit_duckdb.py`) — confirmed neither had location data
2. Researched Apple Health export structure — GPS data is in separate GPX files in `workout-routes/` folder
3. Designed approach: store `start_lat`/`start_lon` in DB, let LLM geocode location queries at runtime
4. Updated `healthkit_to_parquet.py` to:
   - Add `start_lat` and `start_lon` columns to schema
   - Parse `WorkoutRoute/FileReference` from Workout elements
   - Extract first `<trkpt>` from GPX files as starting coordinates
5. Ran export — fast (27.6s for 5.6M rows) but GPS linking failed

## Architecture
```
Claude Desktop (orchestrator)
    ├── healthkit-mcp-v2  →  NLQ→SQL  →  Ollama/Qwen (local)
    └── log-analyzer-mcp  →  failure analysis  →  Opus 4.5 (API)
                          →  semantic review   →  Opus 4.5 (API)
```

## Key Files
- **Parquet exporter:** `~/PycharmProjects/healthkit_export_ripper_one_table/healthkit_to_parquet.py`
- **Export source:** `~/Downloads/apple_health_export/export.xml`
- **GPX files:** `~/Downloads/apple_health_export/workout-routes/` (2,367 files)
- **Output:** `~/PycharmProjects/healthkit_export_ripper_one_table/health.parquet`

## Export Output (Latest Run)
```
Records:  5,628,491
Workouts: 3,361 (0 with GPS)
Skipped:  1,843
File size: 49.2 MB
Time: 27.6s (~204K rows/sec)
```

## Schema (Current)
```
type, value, value_category, unit, start_date, end_date, 
duration_min, distance_km, energy_kcal, source_name, 
start_lat, start_lon
```

## Debug Command to Try Next
Inspect a Workout element in export.xml to see actual structure:
```bash
grep -A 20 "<Workout" ~/Downloads/apple_health_export/export.xml | head -50
```

Or check if WorkoutRoute exists at all:
```bash
grep "WorkoutRoute" ~/Downloads/apple_health_export/export.xml | head -10
```