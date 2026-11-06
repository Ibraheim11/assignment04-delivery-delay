"""Read-only metadata and partition observations inside the real Airflow container."""
import hashlib
import json
from pathlib import Path
from airflow.models.dagrun import DagRun
from airflow.models.backfill import Backfill
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.utils.session import create_session


def metadata():
    with create_session() as session:
        runs = session.query(DagRun).filter(DagRun.dag_id.in_(['data_pipeline', 'ml_training_pipeline'])).all()
        result = []
        for run in runs:
            tasks = session.query(TaskInstance).filter_by(dag_id=run.dag_id, run_id=run.run_id).all()
            history = session.query(TaskInstanceHistory).filter_by(dag_id=run.dag_id, run_id=run.run_id).all()
            def task(t):
                return {key: str(getattr(t, key)) if getattr(t, key) is not None else None
                        for key in ['task_id', 'state', 'try_number', 'start_date', 'end_date', 'hostname', 'pid', 'queued_dttm']}
            result.append({'dag_id': run.dag_id, 'run_id': run.run_id, 'state': run.state,
                           'run_type': str(run.run_type),
                           'logical_date': str(run.logical_date),
                           'data_interval_start': str(run.data_interval_start),
                           'data_interval_end': str(run.data_interval_end),
                           'tasks': [task(t) for t in tasks], 'history': [task(t) for t in history]})
        return result


def files():
    import pandas as pd
    root = Path('/opt/airflow')
    result = {}
    for layer in ['raw', 'bronze', 'silver', 'gold']:
        for path in sorted((root / 'data' / layer).glob('orders_*.csv')):
            frame = pd.read_csv(path)
            result[str(path.relative_to(root))] = {
                'rows': len(frame), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'duplicate_order_ids': int(frame['order_id'].duplicated().sum())}
    return result


def backfills():
    with create_session() as session:
        return [{key: str(getattr(b, key)) if getattr(b, key) is not None else None
                 for key in ['id', 'dag_id', 'from_date', 'to_date', 'completed_at']}
                for b in session.query(Backfill).filter_by(dag_id='data_pipeline').all()]


if __name__ == '__main__':
    print('RUNTIME_JSON=' + json.dumps({'runs': metadata(), 'files': files(), 'backfills': backfills()}, sort_keys=True))
