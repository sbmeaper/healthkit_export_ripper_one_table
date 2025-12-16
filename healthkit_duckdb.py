#!/usr/bin/env python3
"""
Apple HealthKit to DuckDB Parser

Parses Apple Health export.xml and loads into a single flat DuckDB table
optimized for LLM-based natural language queries.
"""

import argparse
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import duckdb


def normalize_type(raw_type: str) -> str:
    """
    Strip verbose Apple prefixes to make types LLM-friendly.

    Examples:
        HKQuantityTypeIdentifierHeartRate → HeartRate
        HKQuantityTypeIdentifierStepCount → StepCount
        HKWorkoutActivityTypeRunning → WorkoutRunning
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
            # For workouts, prepend "Workout" to distinguish from records
            if prefix == "HKWorkoutActivityType":
                return f"Workout{result}"
            return result
    return raw_type


def parse_timestamp(ts_str: str) -> datetime | None:
    """Parse Apple Health timestamp format."""
    if not ts_str:
        return None
    try:
        # Format: 2024-01-15 08:30:00 -0600
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


def create_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the health table schema."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS health (
            type VARCHAR,
            value DOUBLE,
            unit VARCHAR,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            duration_min DOUBLE,
            distance_km DOUBLE,
            energy_kcal DOUBLE,
            source_name VARCHAR
        )
    """)


def parse_and_load(xml_path: Path, db_path: Path, batch_size: int = 50000, progress_interval: int = 100000) -> dict:
    """
    Parse export.xml and load into DuckDB.

    Uses streaming parser with element clearing to handle large files
    with flat memory usage.

    Returns dict with counts of records and workouts processed.
    """
    conn = duckdb.connect(str(db_path))
    create_table(conn)

    batch = []
    stats = {"records": 0, "workouts": 0, "errors": 0}
    start_time = time.time()
    last_progress = 0

    def total_rows():
        return stats["records"] + stats["workouts"]

    def print_progress():
        elapsed = time.time() - start_time
        total = total_rows()
        rate = total / elapsed if elapsed > 0 else 0
        print(f"  Processed {total:,} rows ({stats['records']:,} records, {stats['workouts']:,} workouts) "
              f"in {elapsed:.1f}s ({rate:,.0f} rows/sec)")

    def flush_batch():
        if batch:
            conn.executemany("""
                INSERT INTO health VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            batch.clear()

    print("Starting parse...")

    # Use iterparse for streaming - only trigger on end events
    context = ET.iterparse(str(xml_path), events=("end",))

    for event, elem in context:
        try:
            if elem.tag == "Record":
                row = (
                    normalize_type(elem.get("type", "")),
                    parse_float(elem.get("value")),
                    elem.get("unit"),
                    parse_timestamp(elem.get("startDate")),
                    parse_timestamp(elem.get("endDate")),
                    None,  # duration_min
                    None,  # distance_km
                    None,  # energy_kcal
                    elem.get("sourceName"),
                )
                batch.append(row)
                stats["records"] += 1

            elif elem.tag == "Workout":
                # Calculate duration in minutes
                duration_min = None
                duration_str = elem.get("duration")
                if duration_str:
                    duration_min = parse_float(duration_str)
                    duration_unit = elem.get("durationUnit", "")
                    if "sec" in duration_unit.lower():
                        duration_min = duration_min / 60 if duration_min else None

                # Get distance (convert to km if needed)
                distance_km = parse_float(elem.get("totalDistance"))
                distance_unit = elem.get("totalDistanceUnit", "")
                if distance_km and "mi" in distance_unit.lower():
                    distance_km *= 1.60934

                row = (
                    normalize_type(elem.get("workoutActivityType", "")),
                    None,  # value
                    None,  # unit
                    parse_timestamp(elem.get("startDate")),
                    parse_timestamp(elem.get("endDate")),
                    duration_min,
                    distance_km,
                    parse_float(elem.get("totalEnergyBurned")),
                    elem.get("sourceName"),
                )
                batch.append(row)
                stats["workouts"] += 1

            # Flush batch periodically
            if len(batch) >= batch_size:
                flush_batch()

            # Progress update
            if total_rows() - last_progress >= progress_interval:
                print_progress()
                last_progress = total_rows()

        except Exception as e:
            stats["errors"] += 1

        # Clear element to keep memory flat
        elem.clear()

    # Final flush
    flush_batch()
    conn.close()

    # Final progress
    print_progress()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Parse Apple Health export.xml into DuckDB"
    )
    parser.add_argument(
        "xml_path",
        type=Path,
        help="Path to export.xml from Apple Health export"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("health.db"),
        help="Output DuckDB file path (default: health.db)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for inserts (default: 50000)"
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

    print(f"\nDone! Loaded into {args.output}")
    print(f"  Records:  {stats['records']:,}")
    print(f"  Workouts: {stats['workouts']:,}")
    if stats["errors"]:
        print(f"  Errors:   {stats['errors']:,}")

    return 0


if __name__ == "__main__":
    exit(main())