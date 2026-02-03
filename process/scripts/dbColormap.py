#!/usr/bin/env python3
"""Create `colormaps` table and populate with cmocean colormaps.

Usage:
  python process/scripts/dbColormap.py [--sample N] [--dry-run] [--recreate]

- --sample N: number of color samples per colormap (default 32)
- --dry-run: do not write to DB, just print SQL / previews
- --recreate: drop table if exists and recreate

Requirements:
- cmocean (preferred) or at least matplotlib
- psycopg2
"""

import os
import sys
import json
import argparse
from datetime import datetime

try:
    import psycopg2
except Exception as e:
    print("psycopg2 required. Install in your environment. Error:", e)
    sys.exit(1)

# prefer cmocean colormaps
try:
    import cmocean.cm as cmo_cm
    have_cmocean = True
except Exception:
    cmo_cm = None
    have_cmocean = False

try:
    import matplotlib
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    have_mpl = True
except Exception:
    have_mpl = False


def rgba_to_hex(rgba):
    # rgba: tuple of floats [0..1]
    r, g, b = [int(255 * float(x)) for x in rgba[:3]]
    return '#{0:02x}{1:02x}{2:02x}'.format(r, g, b)


def get_cmocean_names():
    # Heuristic: list attributes of cmocean.cm that are colormap objects
    names = []
    if not have_cmocean:
        return names
    for name in dir(cmo_cm):
        if name.startswith('_'):
            continue
        obj = getattr(cmo_cm, name)
        # a colormap in cmocean is an instance of matplotlib.colors.Colormap
        try:
            if have_mpl and isinstance(obj, matplotlib.colors.Colormap):
                names.append(name)
        except Exception:
            # skip anything unexpected
            pass
    # return sorted unique
    return sorted(set(names))


def sample_colormap(cmap_obj, n=32):
    # returns list of [pos, '#rrggbb'] with pos normalized 0..1
    stops = []
    for i in range(n):
        pos = i / (n - 1) if n > 1 else 0.0
        rgba = cmap_obj(pos)
        stops.append([round(pos, 6), rgba_to_hex(rgba)])
    return stops


def make_table_sql():
    return """
CREATE TABLE IF NOT EXISTS colormaps (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    stops JSONB NOT NULL,
    type TEXT NOT NULL DEFAULT 'linear',
    mode TEXT NOT NULL DEFAULT 'normalized',
    meta JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""


def insert_sql():
    return "INSERT INTO colormaps (name, description, stops, type, mode, meta) VALUES (%s, %s, %s::jsonb, %s, %s, %s::jsonb) ON CONFLICT (name) DO UPDATE SET stops = EXCLUDED.stops, type = EXCLUDED.type, mode = EXCLUDED.mode, meta = EXCLUDED.meta;"


def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'db'),
        port=os.getenv('PGPORT', '5432'),
        dbname=os.getenv('PGDATABASE', 'oa'),
        user=os.getenv('PGUSER', 'postgres'),
        password=os.getenv('PGPASSWORD', 'postgres'),
        connect_timeout=5
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sample', type=int, default=32, help='samples per colormap')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--recreate', action='store_true', help='drop and recreate table')
    args = parser.parse_args()

    cmap_names = get_cmocean_names()

    print(f'Found {len(cmap_names)} cmocean colormaps')

    rows = []
    for name in cmap_names:
        try:
            cmap = getattr(cmo_cm, name)
            stops = sample_colormap(cmap, n=args.sample)
            # description can be empty; keep minimal metadata
            meta = {'source': 'cmocean', 'generated': datetime.utcnow().isoformat()}
            rows.append((name, f'cmocean:{name}', json.dumps(stops), 'linear', 'normalized', json.dumps(meta)))
        except Exception as e:
            print(f'  Skipping {name}: {e}')

    if args.dry_run:
        print('\nDRY-RUN: SQL to create table:\n')
        print(make_table_sql())
        print('\nPrepared inserts (first 3):')
        for r in rows[:3]:
            print(r)
        print('\nTotal colormaps prepared:', len(rows))
        return

    # write to DB
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            if args.recreate:
                cur.execute('DROP TABLE IF EXISTS colormaps CASCADE;')
            cur.execute(make_table_sql())
            ins = insert_sql()
            for r in rows:
                cur.execute(ins, r)
        conn.commit()
        print(f'Inserted/updated {len(rows)} colormaps into colormaps table')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
