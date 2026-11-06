"""Manual Gold-only ML training using the public Airflow 3.1 TaskFlow API."""
from datetime import timedelta
import logging
from pathlib import Path
import sys

import pendulum
from airflow.sdk import dag, task, get_current_context

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@dag(dag_id='ml_training_pipeline', schedule=None,
     start_date=pendulum.datetime(2026, 10, 1, tz='UTC'), catchup=False,
     max_active_runs=1, default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
     tags=['assignment04', 'ml'])
def build_ml_training_pipeline():
    @task
    def load_gold_data():
        from src import train
        context = get_current_context()
        conf = context['dag_run'].conf or {}
        dates = conf.get('dates')
        if dates is not None and (not isinstance(dates, list) or not dates):
            raise ValueError('dates must be a nonempty list of ISO dates')
        return train.prepare_run(dates=dates, run_id=context['run_id'])

    @task
    def train_model(directory):
        from src.train import train_run
        return train_run(directory)

    @task
    def evaluate_model(directory):
        from src.train import evaluate_run
        return evaluate_run(directory)

    @task
    def save_model(directory):
        from src.train import publish_run
        return publish_run(directory)

    save_model(evaluate_model(train_model(load_gold_data())))


ml_training_pipeline = build_ml_training_pipeline()
