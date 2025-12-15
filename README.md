# Apple HealthKit to DuckDB Parser

## Goal
Python script to parse Apple Health export.xml and load into DuckDB for NLQ (natural language query) via an MCP server.

## Schema Decision: Single Table
One flat table called `health`. This optimizes for LLM SQL generation — fewer tables means fewer decision points where the LLM can pick the wrong table.

## Table Schema
```sql
CREATE TABLE health (
    type VARCHAR,           -- e.g., 'HeartRate', 'StepCount', 'WorkoutRunning'
    value DOUBLE,           -- metric value (NULL for workouts)
    unit VARCHAR,           -- e.g., 'count/min', 'km', 'kg'
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    duration_min DOUBLE,    -- workout duration (NULL for records)
    distance_km DOUBLE,     -- workout distance (NULL for records)
    energy_kcal DOUBLE,     -- workout energy burned (NULL for records)
    source_name VARCHAR
)
```

NULLs are fine — DuckDB is columnar so they compress well, and queries always filter by type anyway.

## XML Parsing Approach
- Use Python `xml.etree.ElementTree.iterparse` for streaming (handles large files)
- Call `elem.clear()` after processing each element to keep memory flat
- Insert directly into DuckDB (no intermediate CSV)
- Parse both `Record` and `Workout` elements into the same table

## Type Name Normalization
Strip verbose Apple prefixes to make types LLM-friendly:
- `HKQuantityTypeIdentifierHeartRate` → `HeartRate`
- `HKQuantityTypeIdentifierStepCount` → `StepCount`
- `HKWorkoutActivityTypeRunning` → `WorkoutRunning`

This makes prompts shorter and more intuitive for LLM SQL generation.

## Input/Output
- Input: `export.xml` (from Apple Health export)
- Output: `health.db` (DuckDB database file)

## Known Issues to Handle
- Apple's export.xml may have malformed DTD or duplicate attributes in some iOS versions
- Use lenient parsing, don't validate against DTD
