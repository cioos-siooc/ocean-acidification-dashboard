"""The ERDDAP and ONC ingestion scripts, as Prefect flows on a schedule.

    python flows.py        # serve both deployments, poll for runs

This is the only module that imports Prefect, so `python erddap_to_ch.py` /
`python onc_to_ch.py` keep working with no server. It adds a scheduler and a
monitoring UI around each script's own `store_sensor()`, nothing else.

### What the UI shows

One flow run per scheduled (or UI-triggered) run of each source, and inside it
**one task run per sensor**, so a sensor whose source is down shows up as a
failed task rather than an "ERROR" line buried in a cron log. A failed sensor
doesn't stop the others; the run is marked Failed if any sensor was.

### Choices worth not undoing

- **Sensors run sequentially**, never `.submit()`ed: the ClickHouse client is
  not thread-safe, and one source's servers don't need hammering in parallel.
- **One run per source at a time, overlaps cancelled** (deployment concurrency
  limit 1, `CANCEL_NEW`) — what `flock -n` did for the old host cron. The next
  run re-fetches from each sensor's last stored timestamp, so nothing is lost.
- **No task retries.** The next hourly run is the retry.

### Pausing

`serve()` re-applies `SENSORS_SCHEDULE_PAUSED` every time it starts, so the UI's
pause toggle lasts only until the container next restarts — for a pause that
holds, stop the `sensors-scheduler` service.
"""

import os

from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.states import Completed, Failed, State

import erddap_to_ch
import onc_to_ch
from ch_helpers import get_ch_client

# The old host cron's times: ERDDAP on the hour, ONC at half past.
DEFAULT_ERDDAP_CRON = '0 * * * *'
DEFAULT_ONC_CRON = '30 * * * *'


def _bool_env(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ('1', 'true', 'yes', 'on')


# The client is a live connection that can't be hashed into a cache key, and
# nothing here is worth caching or persisting.
_TASK_OPTS = dict(cache_policy=NO_CACHE, persist_result=False)


@task(name='erddap-sensor', task_run_name='{sensor[name]}', **_TASK_OPTS)
def erddap_sensor(client, sensor: dict) -> None:
    erddap_to_ch.store_sensor(client, sensor)


@task(name='onc-sensor', task_run_name='{sensor[name]}', **_TASK_OPTS)
def onc_sensor(client, sensor: dict, date_to: str) -> None:
    onc_to_ch.store_sensor(client, sensor, date_to)


def _outcome(states: list[tuple[str, State]]) -> State:
    """Failed if any sensor's task was; the rest still ran."""
    failed = [name for name, state in states if state.is_failed()]
    if failed:
        return Failed(message=f'{len(failed)} of {len(states)} sensor(s) failed: ' + ', '.join(failed))
    return Completed(message=f'{len(states)} sensor(s) stored')


@flow(name='oceaneco-sensors-erddap', log_prints=True)
def erddap_sensors(sensor_id: str | None = None) -> State:
    """Fetch every active ERDDAP sensor's new data into sensor_timeseries (or
    only `sensor_id`)."""
    client = get_ch_client()
    try:
        sensors = erddap_to_ch.get_active_erddap_sensors(client, sensor_id)
        return _outcome([(s['name'], erddap_sensor(client, s, return_state=True)) for s in sensors])
    finally:
        client.close()


@flow(name='oceaneco-sensors-onc', log_prints=True)
def onc_sensors(sensor_id: str | None = None) -> State:
    """Fetch every active ONC sensor's new data into sensor_timeseries (or only
    `sensor_id`)."""
    client = get_ch_client()
    try:
        sensors = onc_to_ch.get_active_onc_sensors(client, sensor_id)
        date_to = onc_to_ch.utc_now_str()
        return _outcome([(s['name'], onc_sensor(client, s, date_to, return_state=True)) for s in sensors])
    finally:
        client.close()


if __name__ == '__main__':
    # Serve the flows as imported from this module by name, not `__main__`'s
    # copies: the deployment's entrypoint is recorded from the flow's module,
    # and `__main__.erddap_sensors` isn't importable by the process that later
    # executes a run.
    from prefect import serve
    from prefect.client.schemas.objects import ConcurrencyLimitConfig, ConcurrencyLimitStrategy
    from prefect.deployments.runner import EntrypointType

    import flows

    paused = _bool_env('SENSORS_SCHEDULE_PAUSED')

    def deploy(fl, name, cron, what):
        return fl.to_deployment(
            # Names and tag carry the OceanECO prefix: the Prefect server is
            # shared with other apps, and all of these are global on it.
            name=name,
            tags=['oceaneco'],
            description=f'{what} sensors -> ClickHouse sensor_timeseries (ocean-acidification-dashboard)',
            cron=cron,
            paused=paused,
            concurrency_limit=ConcurrencyLimitConfig(
                limit=1, collision_strategy=ConcurrencyLimitStrategy.CANCEL_NEW,
            ),
            entrypoint_type=EntrypointType.MODULE_PATH,
        )

    serve(
        deploy(flows.erddap_sensors, 'OceanECO-sensors-ERDDAP',
               os.environ.get('ERDDAP_CRON', DEFAULT_ERDDAP_CRON), 'ERDDAP'),
        deploy(flows.onc_sensors, 'OceanECO-sensors-ONC',
               os.environ.get('ONC_CRON', DEFAULT_ONC_CRON), 'ONC'),
        limit=2,
    )
