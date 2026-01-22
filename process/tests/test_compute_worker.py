from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
import os

from dl2pkg.compute import process_pending_compute


def test_process_pending_compute_runs_subprocess_and_updates(monkeypatch, tmp_path):
    conn = MagicMock()
    # make find_pending_compute_groups return one group: (ds_id, start_time, end_time, ids)
    ds_id = 1
    start = datetime(2026,1,15,0,30,tzinfo=timezone.utc)
    end = datetime(2026,1,15,23,30,tzinfo=timezone.utc)
    ids = [123, 124, 125]
    monkeypatch.setattr('dl2pkg.compute.find_pending_compute_groups', lambda conn, limit=10: [(ds_id, start, end, ids)])

    # Mock subprocess.run to succeed
    monkeypatch.setattr('subprocess.run', lambda cmd, check=False: MagicMock(returncode=0))

    # Create expected output files in temp base_dir
    base_dir = tmp_path
    ph_dir = base_dir / 'ph_total'
    ph_dir.mkdir()
    fname = 'dissolved_inorganic_carbon_20260115T0030_20260115T2330.nc'.replace('dissolved_inorganic_carbon', 'ph_total')
    fpath = ph_dir / fname
    fpath.write_bytes(b'netcdf')

    # Make DB query for DIC filename return our dic filename
    cur = MagicMock()
    def cursor_cm():
        cm = MagicMock()
        cm.__enter__.return_value = cur
        cm.__exit__.return_value = False
        return cm
    conn.cursor.side_effect = cursor_cm

    # First SELECT pg_try_advisory_lock -> True (lock id will be min(ids) == 123)
    # Next SELECT for dic filename -> return (dic_filename, dic_path)
    cur.fetchone.side_effect = [(True,), ('dissolved_inorganic_carbon_20260115T0030_20260115T2330.nc', str(base_dir / 'dissolved_inorganic_carbon' / 'dummy.nc'))]
    process_pending_compute(conn, dry_run=False, workers=1, limit=10, base_dir=str(base_dir))

    # Ensure we updated computed rows (we expect an UPDATE call marking success_compute)
    assert any("status='success_compute'" in str(c) for c in cur.execute.call_args_list)
