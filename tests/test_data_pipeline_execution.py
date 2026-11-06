"""Opt-in real DagRun tests: RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q.

Uses Airflow's configured metadata DB and real in-process Execution API. No scheduler
or worker service is needed. Run only against a development Airflow database.
"""
from datetime import timedelta
import os

import pendulum
import pytest

from dags.data_pipeline import build_data_pipeline
from src import etl

pytestmark = pytest.mark.skipif(os.environ.get('RUN_AIRFLOW_INTEGRATION') != '1',
                               reason='requires development Airflow metadata DB')
REPO = etl.ROOT
TASKS = ['extract_to_bronze', 'transform_to_silver', 'prepare_gold', 'validate_gold']


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    return tmp_path


def execute(day, dag=None):
    dag = dag or build_data_pipeline()
    result = dag.test(logical_date=pendulum.parse(day, tz='UTC'), run_conf={'batch_date': day})
    return result.state, {ti.task_id: ti.state for ti in result.get_task_instances()}


@pytest.mark.parametrize('day,rows', [('2026-10-01', 180), ('2026-10-18', 245),
                                    ('2026-10-19', 180), ('2026-10-20', 193)])
def test_real_run_and_rerun(isolated, day, rows):
    for other in ['2026-10-01', '2026-10-18', '2026-10-19', '2026-10-20']:
        target = etl.path_for('raw', other)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REPO / 'data/raw' / target.name).read_bytes())
    for _ in range(2):
        state, tasks = execute(day)
        assert state == 'success'
        assert tasks == dict.fromkeys(TASKS, 'success')
        assert etl.validate_gold(day)['rows'] == rows
        for layer in ['bronze', 'silver', 'gold']:
            assert list((isolated / 'data' / layer).glob('*.csv')) == [etl.path_for(layer, day)]
        current = {p.relative_to(isolated): p.read_bytes() for p in isolated.rglob('*.csv')}
        if _ == 0:
            before = current
        else:
            assert before == current


@pytest.mark.parametrize('stage', TASKS)
def test_real_failure_blocks_dependents(isolated, monkeypatch, stage):
    day = '2026-10-01'
    target = etl.path_for('raw', day)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((REPO / 'data/raw' / target.name).read_bytes())
    def fail(batch_date):
        raise ValueError('intentional failure propagation test')
    monkeypatch.setattr(etl, stage, fail)
    dag = build_data_pipeline()
    # Exhaustion semantics without waiting for the production two five-minute retries.
    for task in dag.tasks:
        task.retries = 0
    state, tasks = execute(day, dag)
    assert state == 'failed'
    index = TASKS.index(stage)
    assert tasks == {name: ('success' if i < index else 'failed' if i == index else 'upstream_failed')
                     for i, name in enumerate(TASKS)}


def test_real_transient_failure_retries(isolated, monkeypatch):
    day = '2026-10-01'
    target = etl.path_for('raw', day)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((REPO / 'data/raw' / target.name).read_bytes())
    original = etl.extract_to_bronze
    calls = []
    def flaky(batch_date):
        calls.append(batch_date)
        if len(calls) == 1:
            raise OSError('intentional transient failure')
        return original(batch_date)
    monkeypatch.setattr(etl, 'extract_to_bronze', flaky)
    dag = build_data_pipeline()
    for task in dag.tasks:
        task.retry_delay = timedelta(0)
    state, tasks = execute(day, dag)
    assert state == 'success'
    assert tasks == dict.fromkeys(TASKS, 'success')
    assert calls == [day, day]
