"""Deprecated module: sublevel processing has been removed from the pipeline."""


def _removed(*_args, **_kwargs):
    raise RuntimeError("sublevel processing has been removed")


load_configs = _removed
load_depth_indices = _removed
find_pending_sublevels = _removed
process_sublevel = _removed
process_pending_sublevels = _removed
