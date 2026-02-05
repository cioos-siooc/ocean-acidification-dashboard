#!/usr/bin/env python3
"""Benchmark timeseries extraction performance.

Measures extract_timeseries run time and resource usage (wall time, CPU time, RSS) when
extracting a point timeseries from full original NC files: DATA_DIR/{var}/*.nc

The script creates a temporary directory with symlinks to the subset of files corresponding
to the *last N distinct dates* found in the filenames (configurable with --days) and then
invokes the same extraction routine used by the API, measuring wall & CPU time and memory RSS.

Examples:
  # Compare last 5 days for var=temperature with 3 repeats (uses DB to map lat/lon -> row/col)
  python process/benchmarks/benchmark_timeseries.py --var temperature --lat 49.2 --lon -123.5 \
      --depth 5 --data-dir /opt/data/nc --days 5 --repeats 3

  # If you prefer to supply row/col (avoid DB lookup):
  python process/benchmarks/benchmark_timeseries.py --var temp --row 12 --col 45 --depth 0 --data-dir /opt/data/nc --days 3

Output: prints per-run metrics and aggregate (median/mean/std) per mode and optionally writes CSV if --out is given.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import shutil
import time
import statistics
import resource
import hashlib
import pathlib
from glob import glob
from typing import List, Optional, Tuple

# Ensure project root is importable so we can import API helper functions
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from extractTimeseries import extract_timeseries, find_variable, find_depth_dim, find_horiz_dims_by_shape, pick_time_slice
except Exception as e:
    raise RuntimeError(f"Could not import extractTimeseries: {e}")

# Optional psutil for more accurate memory metrics
try:
    import psutil
except Exception:
    psutil = None


def select_recent_files(base_dir: str, var: str, days: int) -> List[str]:
    """Return list of file paths for the last `days` distinct date tokens found in filenames."""
    d0 = os.path.join(base_dir, var)
    if not os.path.isdir(d0):
        return []
    files = sorted(glob(os.path.join(d0, "*.nc")))
    if not files:
        return []

    date_tokens = []
    import re
    for fp in files:
        bn = os.path.basename(fp)
        m = re.search(r"(\d{8})T\d{4}", bn)
        if m:
            date_tokens.append(m.group(1))
            continue
        m2 = re.search(r"(\d{8})", bn)
        if m2:
            date_tokens.append(m2.group(1))
    if not date_tokens:
        return files

    from datetime import datetime as _dt
    unique_dates = sorted({_dt.strptime(t, "%Y%m%d").date() for t in date_tokens})
    last_n = unique_dates[-days:]
    last_tokens = {d.strftime("%Y%m%d") for d in last_n}
    selected = [f for f in files if any(tok in os.path.basename(f) for tok in last_tokens)]
    return selected


def make_symlink_subset(selected: List[str], var: str) -> str:
    tmp = tempfile.mkdtemp(prefix=f"bench_{var}_")
    dest = os.path.join(tmp, var)
    os.makedirs(dest, exist_ok=True)
    for fp in selected:
        dest_fp = os.path.join(dest, os.path.basename(fp))
        try:
            os.symlink(fp, dest_fp)
        except Exception:
            shutil.copy(fp, dest_fp)
    return tmp


def measure(func, *args, **kwargs):
    """Measure wall time, CPU time and peak RSS (best-effort)."""
    # initial metrics
    t0 = time.perf_counter()
    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    if psutil:
        proc = psutil.Process()
        rss0 = proc.memory_info().rss
    else:
        rss0 = ru0.ru_maxrss

    try:
        res = func(*args, **kwargs)
        ok = True
    except Exception as e:
        res = e
        ok = False

    t1 = time.perf_counter()
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    if psutil:
        rss1 = proc.memory_info().rss
    else:
        rss1 = ru1.ru_maxrss

    wall = t1 - t0
    cpu = (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime)
    # rss: use difference of current RSS (not perfect but useful)
    rss = max(0, rss1 - rss0)
    return {
        'ok': ok,
        'result': res,
        'wall': wall,
        'cpu': cpu,
        'rss': rss,
    }


def run_single_extract_with_symlinked_dir(base_dir: str, var: str, days: int, extra_args: dict) -> dict:
    """Prepare symlink subset and call extract_timeseries to benchmark it."""
    selected = select_recent_files(base_dir, var, days)
    if not selected:
        raise RuntimeError(f"No files found for mode={mode} var={var} in {base_dir}")
    tmpdir = make_symlink_subset(selected, var)
    try:
        # call extract_timeseries with data_dir pointing to tmpdir so it only sees the subset
        kwargs = {**extra_args, 'data_dir': tmpdir}
        out = measure(extract_timeseries, **kwargs)
        out['n_files'] = len(selected)
        out['mode'] = 'full'
        out['tmpdir'] = tmpdir
        return out
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_bench(args):
    results = {'full': []}

    extra = {
        'var': args.var,
        'lat': args.lat if args.lat is not None else 0.0,
        'lon': args.lon if args.lon is not None else 0.0,
        'depth': args.depth,
        'db_dsn': args.db_dsn,
        'db_host': args.db_host,
        'db_port': args.db_port,
        'db_user': args.db_user,
        'db_password': args.db_password,
        'db_name': args.db_name,
        'db_table': args.db_table,
        'verbose': False,
    }

    # If row/col are provided we won't use DB: in that case, call a small local extraction
    # wrapper that uses the same file subset approach but does selection by indices.
    if args.row is not None and args.col is not None:
        def local_extract(data_dir, var, row, col, depth):
            # replicate minimal logic of extract_timeseries but select by row/col
            files = sorted(glob(os.path.join(data_dir, var, "*.nc")))
            times_list = []
            values_list = []
            for fp in files:
                try:
                    with xr.open_dataset(fp) as dsf:
                        varf = find_variable(dsf, var)
                        tdim = None
                        for d in varf.dims:
                            if d.lower() in ("time",):
                                tdim = d
                                break
                        if tdim is None:
                            continue
                        idxs_local, times_local = pick_time_slice(dsf, tdim, None, None)
                        # find horizontal dims
                        y_dim, x_dim = find_horiz_dims_by_shape(varf, dsf.dims[next(iter(dsf.dims))] if False else dsf[varf.name].sizes[ varf.dims[-2] ], dsf[varf.name].sizes[varf.dims[-1]])
                        # easier: try heuristic
                        y_dim, x_dim = None, None
                        for d in varf.dims:
                            if varf.sizes[d] == dsf.dims[varf.dims[-2]] and y_dim is None:
                                y_dim = d
                            elif varf.sizes[d] == dsf.dims[varf.dims[-1]] and x_dim is None:
                                x_dim = d
                        sel = {tdim: idxs_local, y_dim: row, x_dim: col}
                        depth_dim = find_depth_dim(varf)
                        if depth_dim is not None:
                            # map requested depth value to nearest index by inspecting depth array
                            try:
                                depths = dsf[depth_dim].values
                                depth_sel = int((abs(depths - depth)).argmin())
                                sel[depth_dim] = depth_sel
                            except Exception:
                                pass
                        sub = varf.isel(sel)
                        vals = sub.values.astype(float)
                        times_list.append(times_local)
                        values_list.append(vals)
                except Exception:
                    continue
            if not times_list:
                raise RuntimeError('No data found for provided selection')
            import pandas as pd
            times_concat = pd.DatetimeIndex([]).append(times_list)
            import numpy as np
            values_concat = np.concatenate(values_list)
            df = pd.DataFrame({"time": times_concat, "value": values_concat})
            df = df.drop_duplicates(subset="time").sort_values(by="time").reset_index(drop=True)
            return df.time, df.value

        # replace extract function used by measure with local wrapper when row/col provided
        for i in range(args.repeats):
            selected = select_recent_files(args.data_dir, args.var, args.days)
            if not selected:
                print(f"No files for var={args.var}")
                continue
            tmpdir = make_symlink_subset(selected, args.var)
            try:
                res = measure(local_extract, tmpdir, args.var, args.row, args.col, args.depth)
                res.update({'mode': 'full', 'n_files': len(selected)})
                results['full'].append(res)
                print(f"Mode=full run {i+1}/{args.repeats}: wall={res['wall']:.3f}s cpu={res['cpu']:.3f}s rss={res['rss']/1024:.1f}KB")
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
    else:
        # DB-backed extract_timeseries runs (lat/lon provided)
        for i in range(args.repeats):
            try:
                out = run_single_extract_with_symlinked_dir(args.data_dir, args.var, args.days, extra)
                results['full'].append(out)
                if out['ok']:
                    print(f"Mode=full run {i+1}/{args.repeats}: wall={out['wall']:.3f}s cpu={out['cpu']:.3f}s rss={out['rss']/1024:.1f}KB (files={out['n_files']})")
                else:
                    print(f"Mode=full run {i+1}/{args.repeats}: FAILED: {out['result']}")
            except Exception as e:
                print(f"Mode=full run {i+1}/{args.repeats}: FAILED SETUP: {e}")

    # Summarize
    print("\n=== Summary ===")
    rows = []
    for m in modes:
        mats = [r for r in results[m] if r.get('ok')]
        if not mats:
            print(f"Mode={m}: no successful runs")
            continue
        walls = [r['wall'] for r in mats]
        cpus = [r['cpu'] for r in mats]
        rsss = [r['rss'] for r in mats]
        print(f"Mode={m}: runs={len(mats)} files={mats[0].get('n_files', '?')}")
        print(f"  wall: median={statistics.median(walls):.3f}s mean={statistics.mean(walls):.3f}s std={statistics.pstdev(walls):.3f}s")
        print(f"  cpu:  median={statistics.median(cpus):.3f}s mean={statistics.mean(cpus):.3f}s std={statistics.pstdev(cpus):.3f}s")
        print(f"  rss:  median={statistics.median(rsss)/1024:.1f}KB mean={statistics.mean(rsss)/1024:.1f}KB std={statistics.pstdev(rsss)/1024:.1f}KB")

    return results


def main(argv=None):
    p = argparse.ArgumentParser(description="Benchmark timeseries extraction (full files)")
    p.add_argument("--var", required=True)
    group = p.add_mutually_exclusive_group()
    group.add_argument("--lat", type=float)
    group.add_argument("--row", type=int)
    p.add_argument("--lon", type=float)
    p.add_argument("--col", type=int)
    p.add_argument("--depth", type=float, required=True)
    p.add_argument("--data-dir", default=os.environ.get('DATA_DIR', '/opt/data/nc'))
    p.add_argument("--days", type=int, default=5, help="How many distinct recent dates to include")
    p.add_argument("--repeats", type=int, default=3)

    # DB connection (used when lat/lon provided and row/col absent)
    p.add_argument("--db-dsn", default=None)
    p.add_argument("--db-host", default=os.environ.get('PGHOST', 'db'))
    p.add_argument("--db-port", default=int(os.environ.get('PGPORT', 5432)), type=int)
    p.add_argument("--db-user", default=os.environ.get('PGUSER', 'postgres'))
    p.add_argument("--db-password", default=os.environ.get('PGPASSWORD', 'postgres'))
    p.add_argument("--db-name", default=os.environ.get('PGDATABASE', 'oa'))
    p.add_argument("--db-table", default='grid')

    args = p.parse_args(argv)

    if args.lat is None and args.row is None:
        p.error('Specify either --lat/--lon (DB lookup) or --row/--col (direct indices)')

    results = run_bench(args)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
