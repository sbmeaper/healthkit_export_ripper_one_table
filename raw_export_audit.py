#!/usr/bin/env python3
"""
Audit Apple Health export.xml to discover all attributes and value patterns.
"""

import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

xml_path = Path("/Users/scottbartlow/PycharmProjects/healthkit_export_ripper_one_table/export.xml")

# Track attributes and sample values for each tag type
tag_attributes = defaultdict(lambda: defaultdict(set))  # tag -> attr -> sample values
tag_counts = defaultdict(int)
type_values = defaultdict(set)  # record type -> distinct values seen

MAX_SAMPLES = 10  # Keep up to N sample values per attribute

print(f"Auditing {xml_path}...")
print("This may take a few minutes for large files.\n")

context = ET.iterparse(str(xml_path), events=("end",))
progress = 0

for event, elem in context:
    if elem.tag in ("Record", "Workout", "ActivitySummary", "Correlation"):
        tag_counts[elem.tag] += 1

        for attr, value in elem.attrib.items():
            samples = tag_attributes[elem.tag][attr]
            if len(samples) < MAX_SAMPLES:
                # Truncate long values for display
                display_val = value[:80] + "..." if len(value) > 80 else value
                samples.add(display_val)

        # Track value by type for Records
        if elem.tag == "Record":
            rec_type = elem.get("type", "")
            rec_value = elem.get("value", "")
            if rec_value and len(type_values[rec_type]) < MAX_SAMPLES:
                type_values[rec_type].add(rec_value[:50])

        progress += 1
        if progress % 500000 == 0:
            print(f"  Scanned {progress:,} elements...")

    elem.clear()

print(f"\nScan complete. {progress:,} elements processed.\n")

# Report findings
print("=" * 70)
print("TAG COUNTS")
print("=" * 70)
for tag, count in sorted(tag_counts.items(), key=lambda x: -x[1]):
    print(f"  {tag}: {count:,}")

for tag in ["Record", "Workout", "ActivitySummary", "Correlation"]:
    if tag not in tag_attributes:
        continue
    print("\n" + "=" * 70)
    print(f"{tag} ATTRIBUTES")
    print("=" * 70)
    for attr, samples in sorted(tag_attributes[tag].items()):
        print(f"\n  {attr}:")
        for sample in list(samples)[:5]:
            print(f"    - {sample}")

# Show types with non-numeric values
print("\n" + "=" * 70)
print("RECORD TYPES WITH NON-NUMERIC VALUES")
print("=" * 70)
for rec_type, values in sorted(type_values.items()):
    non_numeric = [v for v in values if not v.replace(".", "").replace("-", "").isdigit()]
    if non_numeric:
        print(f"\n  {rec_type}:")
        for v in list(non_numeric)[:5]:
            print(f"    - {v}")