#!/usr/bin/env python3
"""
Apple HealthKit to Parquet Parser (v2 - with GPS coordinates)

Parses Apple Health export.xml and writes to a Parquet file.
Extracts starting GPS coordinates from workout route GPX files.
"""

import argparse
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

# GPX namespace - Apple uses GPX 1.1
GPX_NS = {"gpx": "http://www.topografix.com/GPX/1/1"}


def normalize_type(raw_type: str) -> str:
    """Strip verbose Apple prefixes to make types LLM-friendly."""
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
    """Strip verbose Apple prefixes from category values."""
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


def parse_value(val: str) -> tuple[float | None, str | None]:
    """Parse value field, returning (numeric_value, category_value)."""
    if not val:
        return None, None
    try:
        return float(val), None
    except ValueError:
        return None, normalize_category_value(val)


def parse_float(val: str) -> float | None:
    """Safely parse a float value."""
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        return None


def get_gpx_start_point(gpx_path: Path) -> tuple[float | None, float | None]:
    """
    Extract the first track point (starting location) from a GPX file.

    Returns (latitude, longitude) or (None, None) if not found.
    """
    if not gpx_path.exists():
        return None, None

    try:
        tree = etree.parse(str(gpx_path))
        root = tree.getroot()

        # Try with namespace first (GPX 1.1)
        trkpt = root.find(".//gpx:trkpt", GPX_NS)

        # Fallback: try without namespace
        if trkpt is None:
            trkpt = root.find(".//{http://www.topografix.com/GPX/1/1}trkpt")

        # Fallback: try with no namespace at all (some GPX files)
        if trkpt is None:
            trkpt = root.find(".//trkpt")

        if trkpt is not None:
            lat = parse_float(trkpt.get("lat"))
            lon = parse_float(trkpt.get("lon"))
            return lat, lon

    except Exception:
        pass

    return None, None


def get_workout_route_path(workout_elem, export_dir: Path) -> Path | None:
    """
    Extract the GPX file path from a Workout element's WorkoutRoute/FileReference.

    Returns the full path to the GPX file, or None if not found.
    """
    # Look for WorkoutRoute child element
    workout_route = workout_elem.find("WorkoutRoute")
    if workout_route is None:
        return None

    # Look for FileReference child
    file_ref = workout_route.find("FileReference")
    if file_ref is None:
        return None

    # Get the path attribute
    rel_path = file_ref.get("path")
    if not rel_path:
        return None

    # Build full path (path is relative to export directory)
    # Strip leading slash - XML paths start with "/" but are relative
    rel_path = rel_path.lstrip("/")
    gpx_path = export_dir / rel_path

    return gpx_path


# Schema for the parquet file - now includes start_lat and start_lon
SCHEMA = pa.schema([
    ("type", pa.string()),
    ("value", pa.float64()),
    ("value_category", pa.string()),
    ("unit", pa.string()),
    ("start_date", pa.string()),
    ("end_date", pa.string()),
    ("duration_min", pa.float64()),
    ("distance_km", pa.float64()),
    ("energy_kcal", pa.float64()),
    ("source_name", pa.string()),
    ("start_lat", pa.float64()),
    ("start_lon", pa.float64()),
])


def parse_and_load(xml_path: Path, parquet_path: Path, batch_size: int = 500000,
                   progress_interval: int = 500000) -> dict:
    """Parse export.xml and write to Parquet."""

    # Determine export directory (parent of export.xml)
    export_dir = xml_path.parent

    # Column arrays for batch processing
    types = []
    values = []
    value_categories = []
    units = []
    start_dates = []
    end_dates = []
    duration_mins = []
    distance_kms = []
    energy_kcals = []
    source_names = []
    start_lats = []
    start_lons = []

    stats = {"records": 0, "workouts": 0, "workouts_with_gps": 0, "skipped": 0, "errors": 0}
    start_time = time.time()
    last_progress = 0
    writer = None

    def total_rows():
        return stats["records"] + stats["workouts"]

    def print_progress():
        elapsed = time.time() - start_time
        total = total_rows()
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"  Processed {total:,} rows ({stats['records']:,} records, "
            f"{stats['workouts']:,} workouts [{stats['workouts_with_gps']:,} with GPS], "
            f"{stats['skipped']:,} skipped) in {elapsed:.1f}s ({rate:,.0f} rows/sec)")

    def flush_batch():
        nonlocal writer, types, values, value_categories, units, start_dates, end_dates
        nonlocal duration_mins, distance_kms, energy_kcals, source_names, start_lats, start_lons

        if not types:
            return

        table = pa.table({
            "type": types,
            "value": values,
            "value_category": value_categories,
            "unit": units,
            "start_date": start_dates,
            "end_date": end_dates,
            "duration_min": duration_mins,
            "distance_km": distance_kms,
            "energy_kcal": energy_kcals,
            "source_name": source_names,
            "start_lat": start_lats,
            "start_lon": start_lons,
        }, schema=SCHEMA)

        if writer is None:
            writer = pq.ParquetWriter(parquet_path, SCHEMA, compression="zstd")

        writer.write_table(table)
        print(f"  Flushed {len(types):,} rows to disk")

        # Clear arrays
        types = []
        values = []
        value_categories = []
        units = []
        start_dates = []
        end_dates = []
        duration_mins = []
        distance_kms = []
        energy_kcals = []
        source_names = []
        start_lats = []
        start_lons = []

    print("Starting parse...")
    print(f"  Export directory: {export_dir}")

    # Check for workout-routes folder
    routes_dir = export_dir / "workout-routes"
    if routes_dir.exists():
        gpx_count = len(list(routes_dir.glob("*.gpx")))
        print(f"  Found workout-routes folder with {gpx_count} GPX files")
    else:
        print(f"  No workout-routes folder found at {routes_dir}")

    context = etree.iterparse(str(xml_path), events=("end",),
                              tag=("Record", "Workout", "ActivitySummary", "Correlation"))

    for event, elem in context:
        try:
            if elem.tag == "Record":
                value_numeric, value_category = parse_value(elem.get("value"))

                types.append(normalize_type(elem.get("type", "")))
                values.append(value_numeric)
                value_categories.append(value_category)
                units.append(elem.get("unit"))
                start_dates.append(elem.get("startDate", "")[:19] if elem.get("startDate") else None)
                end_dates.append(elem.get("endDate", "")[:19] if elem.get("endDate") else None)
                duration_mins.append(None)
                distance_kms.append(None)
                energy_kcals.append(None)
                source_names.append(elem.get("sourceName"))
                start_lats.append(None)  # Records don't have GPS
                start_lons.append(None)
                stats["records"] += 1

            elif elem.tag == "Workout":
                duration_min = None
                duration_str = elem.get("duration")
                if duration_str:
                    duration_min = parse_float(duration_str)
                    duration_unit = elem.get("durationUnit", "")
                    if duration_min and "sec" in duration_unit.lower():
                        duration_min = duration_min / 60

                distance_km = parse_float(elem.get("totalDistance"))
                distance_unit = elem.get("totalDistanceUnit", "")
                if distance_km and "mi" in distance_unit.lower():
                    distance_km *= 1.60934

                # Extract GPS starting point from workout route
                start_lat, start_lon = None, None
                gpx_path = get_workout_route_path(elem, export_dir)
                if gpx_path:
                    start_lat, start_lon = get_gpx_start_point(gpx_path)
                    if start_lat is not None:
                        stats["workouts_with_gps"] += 1

                types.append(normalize_type(elem.get("workoutActivityType", "")))
                values.append(None)
                value_categories.append(None)
                units.append(None)
                start_dates.append(elem.get("startDate", "")[:19] if elem.get("startDate") else None)
                end_dates.append(elem.get("endDate", "")[:19] if elem.get("endDate") else None)
                duration_mins.append(duration_min)
                distance_kms.append(distance_km)
                energy_kcals.append(parse_float(elem.get("totalEnergyBurned")))
                source_names.append(elem.get("sourceName"))
                start_lats.append(start_lat)
                start_lons.append(start_lon)
                stats["workouts"] += 1

            elif elem.tag in ("ActivitySummary", "Correlation"):
                stats["skipped"] += 1

            # Flush periodically
            if len(types) >= batch_size:
                flush_batch()

            # Progress update
            if total_rows() - last_progress >= progress_interval:
                print_progress()
                last_progress = total_rows()

        except Exception as e:
            stats["errors"] += 1

        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    # Final flush
    flush_batch()
    print_progress()

    if writer:
        writer.close()

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Parse Apple Health export.xml into Parquet (with GPS coordinates)"
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
        default=500000,
        help="Rows per batch (default: 500000)"
    )
    parser.add_argument(
        "--progress",
        type=int,
        default=500000,
        help="Print progress every N rows (default: 500000)"
    )

    args = parser.parse_args()

    if not args.xml_path.exists():
        print(f"Error: {args.xml_path} not found")
        return 1

    print(f"Parsing {args.xml_path}...")
    stats = parse_and_load(args.xml_path, args.output, args.batch_size, args.progress)

    print(f"\nDone! Wrote {args.output}")
    print(f"  Records:  {stats['records']:,}")
    print(f"  Workouts: {stats['workouts']:,} ({stats['workouts_with_gps']:,} with GPS)")
    print(f"  Skipped:  {stats['skipped']:,}")
    if stats["errors"]:
        print(f"  Errors:   {stats['errors']:,}")

    size_mb = args.output.stat().st_size / (1024 * 1024)
    print(f"  File size: {size_mb:.1f} MB")

    return 0


if __name__ == "__main__":
    exit(main())