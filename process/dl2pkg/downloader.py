"""Download helper module (contains the download flow)."""
from typing import List
import logging
from .db import get_db_conn

logger = logging.getLogger("dl2.downloader")


def find_pending_rows(conn, limit=10):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT j.id, j.dataset_id, v.variable, j.start_time, j.end_time, j.status, j.attempts, j.nc_path FROM nc_jobs j JOIN erddap_variables v ON v.id = j.variable_id WHERE j.status='pending_download' ORDER BY j.start_time LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        results = []
        for r in rows:
            results.append({
                'id': r[0], 'dataset_id': r[1], 'variable': r[2], 'start_time': r[3], 'end_time': r[4], 'status': r[5], 'attempts': r[6], 'nc_path': r[7]
            })
        return results


def requeue_failed(conn, dataset=None, date=None, variable=None, dry_run=False):
    """Reset failed download rows to pending.

    If dataset/date/variable are provided, restrict the update accordingly.
    If dry_run=True, return the count without performing the update.
    """
    clauses = ["status='failed_download'"]
    params = []
    if dataset:
        # find dataset id
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM erddap_datasets WHERE dataset_id=%s", (dataset,))
            r = cur.fetchone()
            if not r:
                return 0
            params.append(r[0])
            clauses.append("dataset_id = %s")
    if date:
        # restrict by start_time date
        clauses.append("start_time = %s")
        from datetime import datetime
        try:
            day = datetime.strptime(date, '%Y-%m-%d').date()
        except Exception:
            raise ValueError('date must be YYYY-MM-DD')
        st = datetime(day.year, day.month, day.day, 0, 30)
        params.append(st)
    if variable:
        # Resolve variable name to variable_id
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM erddap_variables WHERE variable = %s", (variable,))
            vrow = cur.fetchone()
            if not vrow:
                return 0
            v_id = vrow[0]
        clauses.append("variable_id = %s")
        params.append(v_id)

    where = " AND ".join(clauses)
    sql_count = f"SELECT COUNT(*) FROM nc_jobs WHERE {where}"
    with conn.cursor() as cur:
        cur.execute(sql_count, tuple(params))
        cnt = cur.fetchone()[0]
    if dry_run:
        logger.info("Dry-run: would requeue %d failed rows", cnt)
        return cnt

    sql_update = f"UPDATE nc_jobs SET status='pending_download', attempts=0, last_error=NULL WHERE {where}"
    with conn.cursor() as cur:
        cur.execute(sql_update, tuple(params))
    conn.commit()
    logger.info("Requeued %d failed rows", cnt)
    return cnt


import os
import requests
import tempfile
import hashlib
import shutil
from datetime import timezone
from .das import fetch_das
from psycopg2.extras import Json


def build_griddap_url(erddap_base, dataset_id, variable, start_iso, end_iso, das_text=None):
    """Construct a griddap URL, attempting to include index ranges for depth/gridY/gridX when available from the DAS.
    Format: {variable}[(start):1:(end)][(depth_min):1:(depth_max)][(gridY_min):1:(gridY_max)][(gridX_min):1:(gridX_max)]
    """
    base = f"{erddap_base.rstrip('/')}/griddap/{dataset_id}.nc?"
    time_slice = f"{variable}[({start_iso}):1:({end_iso})]"

    range_suffix = ""
    if das_text:
        import re
        def _find_range(name):
            m = re.search(rf"{name}\s*\{{[^}}]*actual_range\s*([0-9.eE+-]+)\s*,\s*([0-9.eE+-]+)\s*;", das_text, flags=re.S)
            if m:
                a, b = m.groups()
                return (a, b)
            return None

        depth_r = _find_range('depth') or _find_range('deptht')
        gx_r = _find_range('gridX') or _find_range('gridx')
        gy_r = _find_range('gridY') or _find_range('gridy')

        parts = []
        if depth_r:
            parts.append(f"[({depth_r[0]}):1:({depth_r[1]})]")
        if gy_r:
            parts.append(f"[({gy_r[0]}):1:({gy_r[1]})]")
        if gx_r:
            parts.append(f"[({gx_r[0]}):1:({gx_r[1]})]")
        range_suffix = "".join(parts)

    url = base + time_slice + range_suffix
    return url


def _compress_netcdf_file(in_path: str, compression: dict) -> None:
    """Compress an existing NetCDF file in-place using xarray with the provided compression dict.

    The function writes a temporary file and replaces the original.
    """
    import xarray as xr

    ds = xr.open_dataset(in_path)
    encoding = {}
    for v in ds.data_vars:
        enc = {}
        if compression.get("zlib"):
            enc["zlib"] = True
            enc["complevel"] = int(compression.get("complevel", 4))
            if compression.get("shuffle"):
                enc["shuffle"] = True
        encoding[v] = enc
    tmp_out = in_path + ".cmp.nc"
    ds.to_netcdf(tmp_out, encoding=encoding)
    ds.close()
    os.replace(tmp_out, in_path)


def download_nc(conn, row, erddap_base, dry_run=False):
    nid = row["id"] if isinstance(row, dict) else row['id']
    ds_id = row["dataset_id"] if isinstance(row, dict) else row['dataset_id']
    variable = row["variable"] if isinstance(row, dict) else row['variable']
    start_time = row["start_time"] if isinstance(row, dict) else row['start_time']
    end_time = row["end_time"] if isinstance(row, dict) else row['end_time']

    # fetch dataset_id string from erddap_datasets
    with conn.cursor() as cur:
        cur.execute("SELECT dataset_id FROM erddap_datasets WHERE id=%s", (ds_id,))
        dataset_id = cur.fetchone()[0]

    das_text = None
    try:
        das_text = fetch_das(erddap_base, dataset_id)
    except Exception:
        pass

    start_iso = start_time.isoformat().replace("+00:00", "Z")
    end_iso = end_time.isoformat().replace("+00:00", "Z")
    url = build_griddap_url(erddap_base, dataset_id, variable, start_iso, end_iso, das_text)

    logger.info("Downloading %s -> %s", url, variable)
    if dry_run:
        logger.info("dry-run: would download %s", url)
        return True

    tmpfd, tmpfn = tempfile.mkstemp(suffix=".nc.part")
    os.close(tmpfd)
    try:
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            h = hashlib.sha256()
            size = 0
            with open(tmpfn, "wb") as fh:
                for chunk in r.iter_content(8192):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    h.update(chunk)
                    size += len(chunk)
        checksum = h.hexdigest()
        nc_root = os.getenv('NC_ROOT', '/opt/data/nc')
        out_dir = os.path.join(nc_root, variable)
        os.makedirs(out_dir, exist_ok=True)
        fn = f"{variable}_{start_time.strftime('%Y%m%dT%H%M')}_{end_time.strftime('%Y%m%dT%H%M')}.nc"
        final_path = os.path.join(out_dir, fn)
        shutil.move(tmpfn, final_path)

        # Optionally compress the downloaded file based on global config
        from .sublevel import load_configs
        cfg = load_configs()
        comp = cfg.get('compression') if isinstance(cfg, dict) else None
        if comp and comp.get('apply_to_downloads'):
            try:
                _compress_netcdf_file(final_path, comp)
                # recompute checksum and size
                h2 = hashlib.sha256()
                size = 0
                with open(final_path, 'rb') as fh:
                    for chunk in iter(lambda: fh.read(8192), b''):
                        h2.update(chunk)
                        size += len(chunk)
                checksum = h2.hexdigest()
            except Exception:
                logger.exception('Failed to compress downloaded file %s', final_path)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='success_download', nc_path=%s, checksum=%s, attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                (final_path, checksum, nid),
            )
            cur.execute(
                "UPDATE erddap_variables SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE dataset_id = %s AND variable = %s",
                (end_time, ds_id, variable),
            )
            cur.execute(
                "UPDATE erddap_datasets SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE id = %s",
                (end_time, ds_id),
            )
        conn.commit()
        logger.info("Downloaded and stored %s (%d bytes) -> %s", fn, size, final_path)
        return True
    except Exception as e:
        logger.exception("Download failed for %s", url)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_download', last_error=%s, attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                (str(e), nid),
            )
        conn.commit()
        try:
            os.remove(tmpfn)
        except Exception:
            pass
        return False


def do_download(conn, erddap_base, dry_run=False, limit=5):
    pending = find_pending_rows(conn, limit=limit)
    if not pending:
        logger.info("No pending files")
        return
    for row in pending:
        locked = False
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (row["id"],))
            locked = cur.fetchone()[0]
            if locked:
                # mark as processing
                cur.execute("UPDATE nc_jobs SET status='downloading', last_attempt = NOW() WHERE id=%s", (row['id'],))
                conn.commit()
        if not locked:
            logger.info("Skipping pending id %s because lock not acquired", row["id"])
            continue
        try:
            success = download_nc(conn, row, erddap_base, dry_run=dry_run)
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (row["id"],))
            if not success:
                logger.warning("Download failed for pending id %s", row["id"])
            else:
                # If download succeeded, potentially create compute rows (backfill/compute triggers)
                try:
                    maybe_create_compute_rows(conn, row['dataset_id'], row['start_time'], row['end_time'])
                except Exception:
                    logger.exception('maybe_create_compute_rows failed')
        except Exception as e:
            logger.exception("Error processing pending id %s", row["id"])
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE nc_files SET status='failed_download', last_error=%s, attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                    (str(e), row["id"]),
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (row["id"],))


def maybe_create_compute_rows(conn, dataset_id, start_time, end_time):
    """Create pending compute rows for a dataset/time-range if all dependent variables exist.

    Returns True if rows were created, False otherwise.
    """
    logger.debug(
        "Checking whether to create compute rows for dataset=%s %s - %s",
        dataset_id,
        start_time,
        end_time,
    )
    # Required variables for compute: dissolved inorganic carbon, total alkalinity, temperature, salinity
    required = [
        "dissolved_inorganic_carbon",
        "total_alkalinity",
        "temperature",
        "salinity",
    ]
    with conn.cursor() as cur:
        for v in required:
            cur.execute(
                "SELECT COUNT(*) FROM erddap_variables WHERE dataset_id = %s AND variable = %s",
                (dataset_id, v),
            )
            cnt = cur.fetchone()[0]
            if cnt == 0:
                logger.debug("Missing dependent variable %s for dataset %s", v, dataset_id)
                return False
        # All dependencies present: create pending_compute row
        # Ensure a 'compute' variable exists and use its id; attempt INSERT RETURNING id first so tests can mock it easily
        cur.execute("INSERT INTO erddap_variables (dataset_id, variable, meta) VALUES (%s,%s,%s) ON CONFLICT (dataset_id, variable) DO NOTHING RETURNING id", (dataset_id, 'compute', Json({'generated': True})))
        r = cur.fetchone()
        if r:
            compute_var_id = r[0]
        else:
            cur.execute("SELECT id FROM erddap_variables WHERE dataset_id=%s AND variable=%s", (dataset_id, 'compute'))
            compute_var_id = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO nc_jobs (dataset_id, variable_id, start_time, end_time, attempts) VALUES (%s, %s, %s, %s, 0) ON CONFLICT DO NOTHING",
            (dataset_id, compute_var_id, start_time, end_time),
        )
        # Explicitly set status in a second statement so tests can assert on the SQL text
        cur.execute(
            "UPDATE nc_jobs SET status='pending_compute' WHERE dataset_id=%s AND variable_id=%s AND start_time=%s AND end_time=%s",
            (dataset_id, compute_var_id, start_time, end_time),
        )
    conn.commit()
    logger.info("Created pending compute row id=%s for dataset=%s", new_id, dataset_id)
    return True
