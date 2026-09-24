"""The `run` command, as a Prefect flow on a schedule.

    python -m SSC.flows        # serve: register the deployment, poll for runs

This is the only module that imports Prefect, so `python -m SSC.cli ...` keeps
working with no server. It adds a scheduler and a monitoring UI and nothing
else: the steps and their order are `cli.pipeline_steps()`, unchanged — a
second copy here would be a second definition of the pipeline.

### What the UI shows

One flow run per scheduled (or UI-triggered) run, and inside it **one task run
per pipeline step** (check, download, check_image, compute, image, promote,
ingest, sync), so a slow or failing stage is visible at a glance. The
pipeline's own `logging` lines (`SalishSeaCast.*`, `nc2tile`) land in each
task's log tab through `PREFECT_LOGGING_EXTRA_LOGGERS`. Each run ends with a
`pipeline-status` artifact: per-status row counts from `SalishSeaCast_status`
over the recent window, plus every failed_* row touched during the run.

The sweep stages mark individual rows failed_* and carry on rather than
raising, so a green task does not mean every row succeeded. The run itself is
marked Failed when any row entered a failed_* state during it — that is the
signal to open the artifact.

### Choices worth not undoing

- **Steps run sequentially**, never `.submit()`ed: each stage consumes the
  previous stage's promotions, and the ClickHouse client is not thread-safe.
- **One run at a time, overlaps cancelled** (deployment concurrency limit 1,
  `CANCEL_NEW`). Two overlapping runs would both claim the same pending rows,
  and queueing instead would let runs longer than the cron interval stack up.
  The next scheduled run picks up whatever the cancelled one would have.
- **No task retries.** Bulk sweeps already retry failed_* rows up to
  `SSC_MAX_RETRY_ATTEMPTS` on the next run, so the next scheduled run is the
  retry by design.

### Pausing

`serve()` re-applies `RUN_SCHEDULE_PAUSED` every time it starts. So the UI's
pause toggle lasts only until the container next restarts — for a pause that
holds, stop the `scheduler` service.
"""

# No `from __future__ import annotations`: Prefect builds a pydantic model from
# the flow's signature, and string annotations leave `dt.date` unresolvable —
# every run then fails parameter validation before it starts.
import datetime as dt
import logging
import os
from typing import Callable

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE
from prefect.states import Completed, Failed, State

from SSC import cli

# Frequent enough that a newly published SalishSeaCast day is picked up within
# a few hours; a run with nothing pending is a handful of cheap queries.
DEFAULT_CRON = '0 */3 * * *'

# How far back the end-of-run status artifact looks.
SUMMARY_DAYS = 7

LOGGERS = ('SalishSeaCast', 'nc2tile')


def _bool_env(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ('1', 'true', 'yes', 'on')


@task(
    name='pipeline-step',
    task_run_name='{name}',
    # The step is a closure over a live ClickHouse client, which cannot be
    # hashed into a cache key; there is nothing worth caching or persisting.
    cache_policy=NO_CACHE,
    persist_result=False,
)
def run_step(name: str, step: Callable[[], object]) -> None:
    result = step()
    if name == 'check' and isinstance(result, list):
        print(f'check: {len(result)} new date(s) queued')


def _status_summary(client, since: dt.datetime) -> tuple[str, int]:
    """Markdown for the end-of-run artifact, and how many rows entered a
    failed_* state at or after `since` (the run's start)."""
    cutoff = (dt.datetime.utcnow().date() - dt.timedelta(days=SUMMARY_DAYS)).isoformat()
    latest = f"""
        SELECT date, variable,
               argMax(status, updated_at)        AS cur_status,
               argMax(attempts, updated_at)      AS cur_attempts,
               argMax(error_message, updated_at) AS err,
               max(updated_at)                   AS last_update
        FROM SalishSeaCast_status
        WHERE date >= toDate('{cutoff}')
        GROUP BY date, variable
    """
    counts = client.query(
        f'SELECT cur_status, count() FROM ({latest}) GROUP BY cur_status ORDER BY cur_status'
    ).result_rows
    failed = client.query(
        f"""
        SELECT date, variable, cur_status, cur_attempts, err FROM ({latest})
        WHERE startsWith(cur_status, 'failed_') AND last_update >= %(since)s
        ORDER BY date, variable
        """,
        parameters={'since': since},
    ).result_rows

    lines = [f'### Status rows, last {SUMMARY_DAYS} days', '',
             '| status | rows |', '|---|---:|']
    lines += [f'| {status} | {n} |' for status, n in counts] or ['| — | 0 |']
    if failed:
        lines += ['', '### Failed during this run', '',
                  '| date | variable | status | attempts | error |', '|---|---|---|---:|---|']
        lines += [f'| {d} | {var} | {status} | {attempts} | {err.replace("|", "/")[:200]} |'
                  for d, var, status, attempts, err in failed]
    return '\n'.join(lines), len(failed)


@flow(name='ssc-pipeline', log_prints=True)
def ssc_pipeline(
    date: dt.date | None = None,
    limit: int = 10,
    workers: int = 4,
    force: bool = False,
    init_days: int = 3,
) -> State:
    """check -> download -> check_image -> compute -> check_image -> image ->
    promote -> ingest -> promote -> sync. Without `date`, sweeps pending rows
    (up to `limit` per step); with `date`, scopes the whole pipeline to that
    one day. `force` acts regardless of current status (date mode only)."""
    # PREFECT_LOGGING_EXTRA_LOGGERS attaches the API handler but sets no level,
    # so these would inherit the root's WARNING and every INFO line would be
    # dropped before reaching the UI.
    for name in LOGGERS:
        logging.getLogger(name).setLevel(logging.INFO)
    # cli's basicConfig puts the root at INFO, which would print every Prefect
    # API call to the container log.
    logging.getLogger('httpx').setLevel(logging.WARNING)

    # Same clock as db._now(), which stamps every status row's updated_at.
    started = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    client = cli._get_client()
    try:
        for name, step in cli.pipeline_steps(client, date_val=date, limit=limit,
                                             workers=workers, force=force,
                                             init_days=init_days):
            run_step(name, step)

        summary, n_failed = _status_summary(client, started)
    finally:
        client.close()

    create_markdown_artifact(key='pipeline-status', markdown=summary,
                             description='SalishSeaCast_status after this run')
    if n_failed:
        return Failed(message=f'{n_failed} status row(s) entered failed_* during this run')
    return Completed(message='All steps completed; no rows failed')


if __name__ == '__main__':
    # Serve the flow as imported from its package, not the one this `__main__`
    # module just defined: the deployment's entrypoint is recorded from the
    # flow's module, and `__main__.ssc_pipeline` is not importable by the
    # process that later executes a run.
    from prefect import serve
    from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
    from prefect.deployments.runner import EntrypointType

    from SSC.flows import ssc_pipeline as served

    deployment = served.to_deployment(
        name='scheduled',
        cron=os.environ.get('RUN_CRON', DEFAULT_CRON),
        paused=_bool_env('RUN_SCHEDULE_PAUSED'),
        parameters={
            'limit': int(os.environ.get('RUN_LIMIT', '10')),
            'workers': int(os.environ.get('RUN_WORKERS', '4')),
        },
        # A run can outlast the cron interval; one that comes due mid-run is
        # cancelled rather than queued, so a slow stretch can't stack a backlog.
        concurrency_limit=ConcurrencyLimitConfig(
            limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
        ),
        entrypoint_type=EntrypointType.MODULE_PATH,
    )
    serve(deployment, limit=1)
