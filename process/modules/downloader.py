"""Download helper module (contains the download flow)."""
from typing import List
import logging
from .db import get_db_conn

logger = logging.getLogger("dl2.downloader")


def find_pending_rows(conn, limit=10, variable=None):
    clauses = ["j.status='pending_download'"]
    params = [limit]
    
    if variable:
        # Map variable name to ID
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM fields WHERE variable = %s", (variable,))
            vrow = cur.fetchone()
            if not vrow:
                return []  # Variable doesn't exist
            v_id = vrow[0]
        clauses.append("j.variable_id = %s")
        params.insert(0, v_id)  # Insert before limit
    
    where = " AND ".join(clauses)
    sql = f"SELECT j.id, j.dataset_id, v.variable, j.start_time, j.end_time, j.status, j.attempts, j.nc_path FROM nc_jobs j JOIN fields v ON v.id = j.variable_id WHERE {where} ORDER BY j.start_time LIMIT %s"
    
    with conn.cursor() as cur:
        # If variable filter is used, params = [v_id, limit]
        # Otherwise params = [limit]
        actual_params = tuple(params)
        cur.execute(sql, actual_params)
        rows = cur.fetchall()
        results = []
        for r in rows:
            results.append({
                'id': r[0], 'dataset_id': r[1], 'variable': r[2], 'start_time': r[3], 'end_time': r[4], 'status': r[5], 'attempts': r[6], 'nc_path': r[7]
            })
        return results


def requeue_failed(conn, dataset=None, date=None, variable=None):
    """Reset failed download rows to pending.

    If dataset/date/variable are provided, restrict the update accordingly.
    """
    clauses = ["status='failed_download'"]
    params = []
    if dataset:
        # find dataset id
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM datasets WHERE dataset_id=%s", (dataset,))
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
            cur.execute("SELECT id FROM fields WHERE variable = %s", (variable,))
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
    sql_update = f"UPDATE nc_jobs SET status='pending_download', attempts=0 WHERE {where}"
    with conn.cursor() as cur:
        cur.execute(sql_update, tuple(params))
    conn.commit()
    logger.info("Requeued %d failed rows", cnt)
    return cnt


import os
import json
import requests
import tempfile
import hashlib
import shutil
from datetime import timezone
from .das import fetch_das
from psycopg2.extras import Json

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "configs.json"))


def load_configs(config_path: str | None = None) -> dict:
    path = config_path or CONFIG_PATH
    try:
        with open(path, "r") as fh:
            return json.load(fh)
    except Exception:
        logger.exception("Failed to load configs from %s", path)
        return {}


def build_griddap_url(base_url, variable, start_iso, end_iso, das_text=None):
    """Construct a griddap URL, attempting to include index ranges for depth/gridY/gridX when available from the DAS.
    Format: {variable}[(start):1:(end)][(depth_min):1:(depth_max)][(gridY_min):1:(gridY_max)][(gridX_min):1:(gridX_max)]
    """
    base = base_url.rstrip('/')
    if base.endswith('.das'):
        base = base[:-4]
    if base.endswith('.nc'):
        base = base[:-3]
    if not base.endswith('.nc?'):
        base = f"{base}.nc?"
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


def _write_compressed_netcdf(in_path: str, out_path: str, compression: dict) -> None:
    """Read uncompressed NetCDF and write compressed version in single operation.

    Reads from in_path, applies compression encoding, writes to out_path.
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
    ds.to_netcdf(out_path, encoding=encoding)
    ds.close()


def download_nc(conn, row, erddap_base):
    nid = row["id"] if isinstance(row, dict) else row['id']
    ds_id = row["dataset_id"] if isinstance(row, dict) else row['dataset_id']
    variable = row["variable"] if isinstance(row, dict) else row['variable']
    start_time = row["start_time"] if isinstance(row, dict) else row['start_time']
    end_time = row["end_time"] if isinstance(row, dict) else row['end_time']

    # fetch base_url from datasets
    with conn.cursor() as cur:
        cur.execute("SELECT base_url FROM datasets WHERE id=%s", (ds_id,))
        base_url = cur.fetchone()[0]

    if not base_url.startswith("http://") and not base_url.startswith("https://"):
        base_url = f"{erddap_base.rstrip('/')}/griddap/{base_url}"

    das_text = None
    try:
        das_text = fetch_das(base_url)
    except Exception:
        pass

    start_iso = start_time.isoformat().replace("+00:00", "Z")
    end_iso = end_time.isoformat().replace("+00:00", "Z")
    url = build_griddap_url(base_url, variable, start_iso, end_iso, das_text)

    logger.info("Downloading %s -> %s", url, variable)
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
        
        # Optionally compress during finalization based on global config
        cfg = load_configs()
        comp = cfg.get('compression') if isinstance(cfg, dict) else None
        if comp and comp.get('apply_to_downloads'):
            try:
                # Read temp file, write compressed to final location (single disk op)
                fn = f"{variable}_{start_time.strftime('%Y%m%d')}.nc"
                final_path = os.path.join(out_dir, fn)
                _write_compressed_netcdf(tmpfn, final_path, comp)
                os.remove(tmpfn)
                # recompute checksum and size from compressed file
                h2 = hashlib.sha256()
                size = 0
                with open(final_path, 'rb') as fh:
                    for chunk in iter(lambda: fh.read(8192), b''):
                        h2.update(chunk)
                        size += len(chunk)
                checksum = h2.hexdigest()
            except Exception:
                logger.exception('Failed to compress and write file')
                # Try to clean up temp file on error
                try:
                    os.remove(tmpfn)
                except:
                    pass
                raise
        else:
            # No compression: just rename temp file to final
            fn = f"{variable}_{start_time.strftime('%Y%m%d')}.nc"
            final_path = os.path.join(out_dir, fn)
            shutil.move(tmpfn, final_path)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='success_download', nc_path=%s, checksum=%s, attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                (final_path, checksum, nid),
            )
            cur.execute(
                "UPDATE fields SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE dataset_id = %s AND variable = %s",
                (end_time, ds_id, variable),
            )
            cur.execute(
                "UPDATE datasets SET last_downloaded_at = GREATEST(COALESCE(last_downloaded_at, to_timestamp(0)), %s) WHERE id = %s",
                (end_time, ds_id),
            )
        conn.commit()
        logger.info("Downloaded and stored %s (%d bytes) -> %s", fn, size, final_path)
        return True
    except Exception as e:
        logger.exception("Download failed for %s", url)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nc_jobs SET status='failed_download', attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                (nid,),
            )
        conn.commit()
        try:
            os.remove(tmpfn)
        except Exception:
            pass
        return False


def do_download(conn, erddap_base, limit=5, variable=None):
    pending = find_pending_rows(conn, limit=limit, variable=variable)
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
            success = download_nc(conn, row, erddap_base)
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (row["id"],))
            if not success:
                logger.warning("Download failed for pending id %s", row["id"])
            # Note: compute rows are now created upfront by check_download, not as a side effect here
        except Exception as e:
            logger.exception("Error processing pending id %s", row["id"])
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE nc_jobs SET status='failed_download', attempts = attempts+1, last_attempt = NOW() WHERE id=%s",
                    (row["id"],),
                )
            conn.commit()
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (row["id"],))



