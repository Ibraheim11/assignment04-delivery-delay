"""Tests use real Airflow 3.1 objects and isolated ETL output directories."""
from datetime import timedelta
from types import SimpleNamespace

import pendulum
import pytest

from dags import data_pipeline as pipeline
from src import etl

DATES = [('2026-10-01', 180), ('2026-10-18', 245),
         ('2026-10-19', 180), ('2026-10-20', 193)]
REPO = etl.ROOT
TASKS = ['extract_to_bronze', 'transform_to_silver', 'prepare_gold', 'validate_gold']


def context(kind='scheduled', conf=None, start='2026-10-18', end='2026-10-19'):
    return {'dag_run': SimpleNamespace(run_type=kind, conf=conf),
            'data_interval_start': pendulum.parse(start, tz='UTC'),
            'data_interval_end': pendulum.parse(end, tz='UTC')}


def test_discovery_and_graph():
    from airflow.models.dagbag import DagBag
    bag = DagBag(dag_folder=str(REPO / 'dags'), include_examples=False)
    assert not bag.import_errors
    dag = bag.get_dag('data_pipeline')
    assert dag is not None
    assert set(dag.task_ids) == set(TASKS)
    assert dag.schedule == '@daily'
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert str(dag.timezone) == 'UTC'
    for i, name in enumerate(TASKS):
        task = dag.get_task(name)
        assert task.upstream_task_ids == (set() if i == 0 else {TASKS[i-1]})
        assert task.downstream_task_ids == (set() if i == 3 else {TASKS[i+1]})
        assert task.trigger_rule == 'all_success'
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=5)


@pytest.mark.parametrize('day,_', DATES)
@pytest.mark.parametrize('kind', ['scheduled', 'backfill'])
def test_automated_resolution(day, _, kind):
    end = pendulum.parse(day).add(days=1).to_date_string()
    assert pipeline.resolve_batch_date(context(kind, start=day, end=end)) == day
    assert pipeline.resolve_batch_date(context(kind, start=end, end=end)) == day


@pytest.mark.parametrize('day,_', DATES)
def test_manual_date_overrides_unrelated_interval(day, _):
    assert pipeline.resolve_batch_date(context('manual', {'batch_date': day})) == day


@pytest.mark.parametrize('value', [None, '', '2026-2-01', '2026-02-30', '../orders', 20261001])
def test_invalid_manual_dates(value):
    with pytest.raises(ValueError):
        pipeline.resolve_batch_date(context('manual', {'batch_date': value}))


def test_missing_manual_date_and_automated_override():
    with pytest.raises(ValueError, match='Manual runs require'):
        pipeline.resolve_batch_date(context('manual'))
    with pytest.raises(ValueError, match='only allowed for manual'):
        pipeline.resolve_batch_date(context(conf={'batch_date': '2026-10-01'}))


@pytest.mark.parametrize('start,end', [('2026-10-17', '2026-10-19'),
    ('2026-10-19', '2026-10-18'), ('2026-10-18T12:00:00Z', '2026-10-19T12:00:00Z')])
def test_invalid_intervals(start, end):
    with pytest.raises(ValueError):
        pipeline.resolve_batch_date(context(start=start, end=end))


@pytest.mark.parametrize('day,expected', DATES)
def test_task_callables_batch_isolation_and_rerun(tmp_path, monkeypatch, day, expected):
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    for other, _ in DATES:
        path = etl.path_for('raw', other)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((REPO / 'data/raw' / path.name).read_bytes())
    monkeypatch.setattr(pipeline, 'get_current_context', lambda: context('manual', {'batch_date': day}))
    dag = pipeline.data_pipeline
    def execute():
        value = dag.get_task(TASKS[0]).python_callable()
        for name in TASKS[1:]:
            value = dag.get_task(name).python_callable(value)
    execute()
    before = {layer: etl.path_for(layer, day).read_bytes() for layer in ['bronze', 'silver', 'gold']}
    execute()
    for layer, content in before.items():
        assert etl.path_for(layer, day).read_bytes() == content
        assert list((tmp_path / 'data' / layer).glob('*.csv')) == [etl.path_for(layer, day)]
    assert before['bronze'] == etl.path_for('raw', day).read_bytes()
    frame = etl.read_frame(etl.path_for('gold', day))
    assert frame.order_date.eq(day).all()
    assert etl.validate_gold(day) == {'batch_date': day, 'rows': expected, 'duplicate_ids': 0}


def test_missing_source_fails_without_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    for day in ['2026-11-16', '2026-10-01']:
        with pytest.raises(FileNotFoundError):
            pipeline.run_stage('extract_to_bronze', day)
    assert not (tmp_path / 'data/bronze').exists()


def test_timetable_daily_boundaries():
    from airflow.timetables.base import TimeRestriction
    timetable = pipeline.data_pipeline.timetable
    info = timetable.next_dagrun_info(last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=pendulum.datetime(2026, 10, 18, tz='UTC'),
                                    latest=None, catchup=True))
    assert info.run_after.hour == 0
    resolved = pipeline.resolve_batch_date(context(start=info.data_interval.start.isoformat(),
                                                  end=info.data_interval.end.isoformat()))
    assert pendulum.parse(resolved).add(days=1) == info.run_after


def test_partition_lock_is_held_during_etl_and_released(tmp_path, monkeypatch):
    import fcntl
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    day = '2026-10-01'
    def guarded_extract(batch_date):
        path = tmp_path / 'data/.locks' / f'{day}.lock'
        with path.open('a') as competing:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise OSError('transient failure')
    monkeypatch.setattr(etl, 'extract_to_bronze', guarded_extract)
    with pytest.raises(OSError, match='transient'):
        pipeline.run_stage('extract_to_bronze', day)
    with (tmp_path / 'data/.locks' / f'{day}.lock').open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
