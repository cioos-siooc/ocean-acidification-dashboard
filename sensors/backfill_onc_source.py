"""
backfill_onc_source.py

One-off / re-runnable backfill: populate `source.description` and
`source.link` in the CH `sensors` table for existing ONC sensors, by
looking up each sensor's device_config.locationCode against the ONC
/locations API.

ONC's `description` field is frequently blank (populated mainly for named
sites, not generic platforms/buoys) — `dataSearchURL` is always present so
it's used as the reference link regardless of whether a description exists.

Usage
-----
    uv run python sensors/backfill_onc_source.py               # apply
    uv run python sensors/backfill_onc_source.py --dry-run      # preview only
    uv run python sensors/backfill_onc_source.py --force        # overwrite existing description/link too
"""

import argparse
import json
import os

from onc import ONC

from ch_helpers import get_ch_client
from manage_sensors import _upsert

TOKEN = os.getenv("ONC_TOKEN", "7d291a6a-b57e-49cd-acb1-83f59010d32b")
onc = ONC(TOKEN)


def get_onc_sensors(ch_client) -> list[dict]:
    rows = ch_client.query(
        "SELECT id, name, latitude, longitude, depth, variables, "
        "device_config, source, active, organization "
        "FROM sensors FINAL "
        "WHERE JSONExtractString(source, 'api') = 'ONC'"
    ).result_rows
    sensors = []
    for r in rows:
        sensors.append({
            "id": str(r[0]),
            "name": r[1],
            "latitude": float(r[2]),
            "longitude": float(r[3]),
            "depth": float(r[4]),
            "variables": json.loads(r[5]) if r[5] else {},
            "device_config": json.loads(r[6]) if r[6] else {},
            "source": json.loads(r[7]) if r[7] else {},
            "active": int(r[8]),
            "organization": r[9] or "",
        })
    return sensors


def main():
    parser = argparse.ArgumentParser(
        description="Backfill source.description/source.link for ONC sensors in CH."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would change without writing to CH.")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite description/link even if already set.")
    args = parser.parse_args()

    ch_client = get_ch_client()
    sensors = get_onc_sensors(ch_client)
    if not sensors:
        print("No ONC sensors found.")
        return

    loc_cache: dict[str, dict | None] = {}
    updated = skipped = errors = 0

    for sensor in sensors:
        location_code = sensor["device_config"].get("locationCode")
        if not location_code:
            print(f"  SKIP   {sensor['name']}  (no locationCode in device_config)")
            skipped += 1
            continue

        if location_code not in loc_cache:
            try:
                result = onc.getLocations({"locationCode": location_code})
                loc_cache[location_code] = result[0] if result else None
            except Exception as exc:
                print(f"  ERROR  {sensor['name']}  ({location_code}): {exc}")
                loc_cache[location_code] = None
                errors += 1
                continue

        loc = loc_cache[location_code]
        if loc is None:
            print(f"  SKIP   {sensor['name']}  ({location_code} not found in ONC API)")
            skipped += 1
            continue

        description = (loc.get("description") or "").strip()
        link = loc.get("dataSearchURL", "")

        source = dict(sensor["source"])
        changed = False

        if link and (args.force or not source.get("link")):
            if source.get("link") != link:
                source["link"] = link
                changed = True
        if description and (args.force or not source.get("description")):
            if source.get("description") != description:
                source["description"] = description
                changed = True

        if not changed:
            print(f"  SKIP   {sensor['name']}  (already up to date)")
            skipped += 1
            continue

        print(f"  UPDATE {sensor['name']}  ({location_code})")
        print(f"           link:        {source.get('link')}")
        print(f"           description: {source.get('description', '')[:80]}")

        if not args.dry_run:
            sensor["source"] = source
            _upsert(ch_client, sensor)
        updated += 1

    verb = "Would update" if args.dry_run else "Updated"
    print(f"\n{verb} {updated} sensor(s), {skipped} skipped, {errors} error(s).")


if __name__ == "__main__":
    main()
