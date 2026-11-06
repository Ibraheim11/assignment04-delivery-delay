"""Daily, single-batch orchestration using the public Airflow 3.1 TaskFlow API."""
from contextlib import contextmanager
from datetime import timedelta
import fcntl
import logging
from pathlib import Path
import sys

import pendulum
from airflow.sdk import dag, get_current_context, task

# Airflow adds dags/ to sys.path; the reusable ETL package lives beside it.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

log = logging.getLogger(__name__)


def resolve_batch_date(context):
    """Manual dates are explicit; automated dates follow the UTC daily timetable."""
    from src.etl import checked_date

    run = context['dag_run']
    run_type = getattr(run.run_type, 'value', run.run_type)
    conf = run.conf or {}
    if run_type == 'manual':
        if 'batch_date' not in conf:
            raise ValueError('Manual runs require dag_run.conf batch_date (YYYY-MM-DD)')
        return checked_date(conf['batch_date'])
    if run_type not in ('scheduled', 'backfill'):
        raise ValueError(f'Unsupported run type: {run_type}')
    if 'batch_date' in conf:
        raise ValueError('batch_date override is only allowed for manual runs')
    start = context['data_interval_start'].in_timezone('UTC')
    end = context['data_interval_end'].in_timezone('UTC')
    midnight = end.start_of('day')
    if end != midnight or start != start.start_of('day'):
        raise ValueError('Expected UTC midnight daily data interval boundaries')
    if end - start == timedelta(days=1):
        return checked_date(start.to_date_string())
    if end == start:
        # Airflow 3.1 defaults to CronTriggerTimetable: a midnight point interval.
        # Its daily run processes the day that just closed, not the trigger day.
        return checked_date(end.subtract(days=1).to_date_string())
    raise ValueError('Expected one daily data interval (or a daily trigger point)')


@contextmanager
def batch_lock(batch_date):
    """Serialize access to one partition, including overlapping backfill jobs."""
    from src import etl

    directory = etl.ROOT / "data" / ".locks"
    directory.mkdir(parents=True, exist_ok=True)
    # Keep the inode: unlinking lock files can let another process bypass a lock.
    with (directory / f"{etl.checked_date(batch_date)}.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_stage(stage, batch_date):
    with batch_lock(batch_date):
        return _run_stage(stage, batch_date)


def _run_stage(stage, batch_date):
    """Keep frames local; existing ETL owns all cleaning and atomic file writes."""
    from src import etl

    batch_date = etl.checked_date(batch_date)
    source_layer = {'extract_to_bronze': 'raw', 'transform_to_silver': 'bronze',
                    'prepare_gold': 'silver', 'validate_gold': 'gold'}[stage]
    source = etl.path_for(source_layer, batch_date)
    log.info('stage=%s processing_date=%s source_file=%s', stage, batch_date, source)
    try:
        result = getattr(etl, stage)(batch_date)
        if stage == 'validate_gold':
            log.info('processing_date=%s gold_path=%s validation=passed results=%s',
                     batch_date, source, result)
        else:
            rows = len(etl.read_frame(result))
            log.info('processing_date=%s output_path=%s rows=%s', batch_date, result, rows)
            if stage == 'transform_to_silver':
                log.info('processing_date=%s quality_report=%s', batch_date,
                         etl.ROOT / 'reports' / 'etl' / f'{batch_date}.json')
    except Exception:
        log.exception('stage=%s processing_date=%s validation_or_stage=failed source_file=%s',
                      stage, batch_date, source)
        raise
    return batch_date


@dag(dag_id='data_pipeline', schedule='@daily',
     start_date=pendulum.datetime(2026, 10, 1, tz='UTC'), catchup=False,
     max_active_runs=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
     tags=['assignment04', 'data', 'medallion'])
def build_data_pipeline():
    @task
    def extract_to_bronze():
        return run_stage('extract_to_bronze', resolve_batch_date(get_current_context()))

    @task
    def transform_to_silver(batch_date):
        return run_stage('transform_to_silver', batch_date)

    @task
    def prepare_gold(batch_date):
        return run_stage('prepare_gold', batch_date)

    @task
    def validate_gold(batch_date):
        run_stage('validate_gold', batch_date)

    validate_gold(prepare_gold(transform_to_silver(extract_to_bronze())))


data_pipeline = build_data_pipeline()
