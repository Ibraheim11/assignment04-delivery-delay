import os
from pathlib import Path
import pendulum
import pytest
from airflow.models.dagbag import DagBag
from dags.ml_training_pipeline import build_ml_training_pipeline
from src import train

TASKS = ['load_gold_data', 'train_model', 'evaluate_model', 'save_model']


def test_discovery_dependencies():
    bag = DagBag(dag_folder=str(train.ROOT / 'dags'), include_examples=False)
    assert not bag.import_errors
    assert {'data_pipeline', 'ml_training_pipeline'} <= set(bag.dags)
    dag = bag.dags['ml_training_pipeline']
    assert dag.schedule is None and dag.max_active_runs == 1
    assert set(dag.task_ids) == set(TASKS)
    for i, name in enumerate(TASKS):
        task = dag.get_task(name)
        assert task.upstream_task_ids == ({TASKS[i-1]} if i else set())
        assert task.downstream_task_ids == ({TASKS[i+1]} if i < 3 else set())
        assert task.trigger_rule == 'all_success'
        assert task.retries == 2


@pytest.mark.skipif(os.environ.get('RUN_AIRFLOW_INTEGRATION') != '1', reason='development Airflow DB required')
@pytest.mark.parametrize('fail_stage', [None] + TASKS)
def test_real_execution_and_failure_propagation(tmp_path, monkeypatch, fail_stage):
    repo = train.ROOT
    monkeypatch.setattr(train, 'ROOT', tmp_path)
    for day in ['2026-10-01', '2026-10-18']:
        target = tmp_path / 'data/gold' / f'orders_{day}.csv'
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repo / 'data/gold' / target.name).read_bytes())
    functions = ['prepare_run', 'train_run', 'evaluate_run', 'publish_run']
    if fail_stage:
        def fail(*args, **kwargs): raise ValueError('intentional ML failure propagation')
        monkeypatch.setattr(train, functions[TASKS.index(fail_stage)], fail)
    dag = build_ml_training_pipeline()
    for task in dag.tasks: task.retries = 0
    result = dag.test(logical_date=pendulum.datetime(2026, 11, 6, tz='UTC'),
                      run_conf={'dates': ['2026-10-01', '2026-10-18']})
    states = {ti.task_id: ti.state for ti in result.get_task_instances()}
    if fail_stage:
        index = TASKS.index(fail_stage)
        assert result.state == 'failed'
        assert states == {name: 'success' if i < index else 'failed' if i == index else 'upstream_failed'
                          for i, name in enumerate(TASKS)}
        assert not (tmp_path / 'models/delivery_delay_model.joblib').exists()
    else:
        assert result.state == 'success'
        assert states == dict.fromkeys(TASKS, 'success')
        assert (tmp_path / 'models/delivery_delay_model.joblib').is_file()
