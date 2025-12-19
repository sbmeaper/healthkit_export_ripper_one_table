#!/usr/bin/env python3
"""
Apple HealthKit to Parquet Parser

Parses Apple Health export.xml and writes to a Parquet file.
DuckDB can query the Parquet file directly.
"""

import argparse
import time
from datetime import datetime
from pathlib import Path

import duckdb
from lxml import etree


def normalize_type(raw_type: str) -> str:
    """
    Strip verbose Apple prefixes to make types LLM-friendly.
    """
    prefixes = [
        "HKQuantityTypeIdentifier",
        "HKCategoryTypeIdentifier",
        "HKDataType",
        "HKWorkoutActivityType",
    ]
    for prefix in prefixes:
        if raw_type.startswith(prefix):
            result = raw_type[len(prefix):]
            if prefix == "HKWorkoutActivityType":
                return f"Workout{result}"
            return result
    return raw_type


def normalize_category_value(raw_value: str) -> str | None:
    """
    Strip verbose Apple prefixes from category values.
    e.g., 'HKCategoryValueSleepAnalysisAsleepCore' -> 'AsleepCore'
    """
    if not raw_value:
        return None

    prefixes = [
        "HKCategoryValueSleepAnalysis",
        "HKCategoryValueAppleStandHour",
        "HKCategoryValueEnvironmentalAudioExposureEvent",
        "HKCategoryValue",
    ]
    for prefix in prefixes:
        if raw_value.startswith(prefix):
            return raw_value[len(prefix):]
    return raw_value


def parse_timestamp(ts_str: str) -> datetime | None:
    """Parse Apple Health timestamp format."""
    if not ts_str:
        return None
    try:
        return datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def parse_float(val: str) -> float | None:
    """Safely parse a float value."""
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def parse_value(val: str) -> tuple[float | None, str | None]:
    """
    Parse a value field, returning (numeric_value, category_value).
    If it's a number, return (number, None).
    If it's a string (category), return (None, normalized_string).
    """
    if not val:
        return None, None

    # Try to parse as float first
    try:
        return float(val), None
    except ValueError:
        # It's a category value
        return None, normalize_category_value(val)


def parse_and_load(xml_path: Path, parquet_path: Path, batch_size: int = 100000,
                   progress_interval: int = 100000) -> dict:
    """
    Parse export.xml and write to Parquet.

    Collects all rows in memory, then writes to Parquet in one pass.
    For very large files, uses chunked writing.
    """

    rows = []
    stats = {"records": 0, "workouts": 0, "skipped": 0, "errors": 0}
    start_time = time.time()
    last_progress = 0
    chunk_num = 0
    temp_parquets = []

    def total_rows():
        return stats["records"] + stats["workouts"]

    def print_progress():
        elapsed = time.time() - start_time
        total = total_rows()
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"  Processed {total:,} rows ({stats['records']:,} records, {stats['workouts']:,} workouts, {stats['skipped']:,} skipped) "
            f"in {elapsed:.1f}s ({rate:,.0f} rows/sec)")

    def flush_to_parquet():
        nonlocal chunk_num, rows
        if not rows:
            return

        conn = duckdb.connect()

        # Create table from rows
        conn.execute("""
            CREATE TABLE chunk (
                type VARCHAR,
                value DOUBLE,
                value_category VARCHAR,
                unit VARCHAR,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                duration_min DOUBLE,
                distance_km DOUBLE,
                energy_kcal DOUBLE,
                source_name VARCHAR
            )
        """)

        conn.executemany("INSERT INTO chunk VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

        # Write chunk to temp parquet
        chunk_path = parquet_path.parent / f".chunk_{chunk_num}.parquet"
        conn.execute(f"COPY chunk TO '{chunk_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        temp_parquets.append(chunk_path)

        conn.close()
        rows = []
        chunk_num += 1
        print(f"  Flushed chunk {chunk_num} to disk")

    print("Starting parse...")

    # Use lxml's iterparse for faster parsing
    context = etree.iterparse(str(xml_path), events=("end",),
                              tag=("Record", "Workout", "ActivitySummary", "Correlation"))

    for event, elem in context:
        try:
            if elem.tag == "Record":
                value_numeric, value_category = parse_value(elem.get("value"))

                row = (
                    normalize_type(elem.get("type", "")),
                    value_numeric,
                    value_category,
                    elem.get("unit"),
                    parse_timestamp(elem.get("startDate")),
                    parse_timestamp(elem.get("endDate")),
                    None,
                    None,
                    None,
                    elem.get("sourceName"),
                )
                rows.append(row)
                stats["records"] += 1

            elif elem.tag == "Workout":
                duration_min = None
                duration_str = elem.get("duration")
                if duration_str:
                    duration_min = parse_float(duration_str)
                    duration_unit = elem.get("durationUnit", "")
                    if "sec" in duration_unit.lower():
                        duration_min = duration_min / 60 if duration_min else None

                distance_km = parse_float(elem.get("totalDistance"))
                distance_unit = elem.get("totalDistanceUnit", "")
                if distance_km and "mi" in distance_unit.lower():
                    distance_km *= 1.60934

                row = (
                    normalize_type(elem.get("workoutActivityType", "")),
                    None,
                    None,
                    None,
                    parse_timestamp(elem.get("startDate")),
                    parse_timestamp(elem.get("endDate")),
                    duration_min,
                    distance_km,
                    parse_float(elem.get("totalEnergyBurned")),
                    elem.get("sourceName"),
                )
                rows.append(row)
                stats["workouts"] += 1

            elif elem.tag in ("ActivitySummary", "Correlation"):
                # Skip computed/derived data
                stats["skipped"] += 1

            # Flush to parquet periodically to avoid memory issues
            if len(rows) >= batch_size:
                flush_to_parquet()

            # Progress update
            if total_rows() - last_progress >= progress_interval:
                print_progress()
                last_progress = total_rows()

        except Exception as e:
            stats["errors"] += 1

        # Clear element and its ancestors to free memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Final flush
    flush_to_parquet()
    print_progress()

    # Merge all chunks into final parquet
    print("Merging chunks into final parquet...")
    conn = duckdb.connect()

    if len(temp_parquets) == 1:
        # Just rename if single chunk
        temp_parquets[0].rename(parquet_path)
    else:
        # Merge multiple chunks
        chunk_pattern = str(parquet_path.parent / ".chunk_*.parquet")
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{chunk_pattern}')) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")

        # Clean up temp files
        for p in temp_parquets:
            p.unlink()

    conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Parse Apple Health export.xml into Parquet"
    )
    parser.add_argument(
        "xml_path",
        type=Path,
        help="Path to export.xml from Apple Health export"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("health.parquet"),
        help="Output Parquet file path (default: health.parquet)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Rows per chunk (default: 100000)"
    )
    parser.add_argument(
        "--progress",
        type=int,
        default=100000,
        help="Print progress every N rows (default: 100000)"
    )

    args = parser.parse_args()

    if not args.xml_path.exists():
        print(f"Error: {args.xml_path} not found")
        return 1

    print(f"Parsing {args.xml_path}...")
    stats = parse_and_load(args.xml_path, args.output, args.batch_size, args.progress)

    print(f"\nDone! Wrote {args.output}")
    print(f"  Records:  {stats['records']:,}")
    print(f"  Workouts: {stats['workouts']:,}")
    print(f"  Skipped:  {stats['skipped']:,}")
    if stats["errors"]:
        print(f"  Errors:   {stats['errors']:,}")

    # Show file size
    size_mb = args.output.stat().st_size / (1024 * 1024)
    print(f"  File size: {size_mb:.1f} MB")

    print(f"\nTo query with DuckDB:")
    print(f"  duckdb -c \"SELECT * FROM '{args.output}' LIMIT 10\"")

    return 0


if __name__ == "__main__":
    exit(main())