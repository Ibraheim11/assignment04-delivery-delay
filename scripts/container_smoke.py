"""Run inside the Compose image to verify versions, DAGs, files and optional outputs."""
import argparse
import hashlib
from importlib.metadata import version
import json
from pathlib import Path

EXPECTED = {'apache-airflow': '3.1.0', 'pandas': '2.1.4', 'numpy': '1.26.4',
            'scikit-learn': '1.7.2', 'joblib': '1.5.2'}
TASKS = {'data_pipeline': ['extract_to_bronze', 'transform_to_silver', 'prepare_gold', 'validate_gold'],
         'ml_training_pipeline': ['load_gold_data', 'train_model', 'evaluate_model', 'save_model']}


def verify(root=None, check_gold=False, baseline=None):
    import joblib
    import numpy as np
    from airflow.models.dagbag import DagBag
    from src import train
    root = Path(root) if root else Path(__file__).resolve().parents[1]
    versions = {name: version(name) for name in EXPECTED}
    if versions != EXPECTED:
        raise ValueError(f'Package version mismatch: {versions}')
    bag = DagBag(dag_folder=str(root / 'dags'), include_examples=False)
    if bag.import_errors:
        raise ValueError(f'DAG import errors: {bag.import_errors}')
    for name, tasks in TASKS.items():
        dag = bag.dags[name]
        assert set(dag.task_ids) == set(tasks)
        for i, task_id in enumerate(tasks):
            assert dag.get_task(task_id).upstream_task_ids == ({tasks[i-1]} if i else set())
    manifest = json.loads((root / 'reports/raw_sha256.json').read_text())
    assert len(manifest) == len(list((root / 'data/raw').glob('orders_*.csv'))) == 46
    assert all(hashlib.sha256((root / path).read_bytes()).hexdigest() == digest
               for path, digest in manifest.items())
    path = root / 'models/delivery_delay_model.joblib'
    model = joblib.load(path)
    metrics = json.loads((root / 'metrics/metrics.json').read_text())
    assert hashlib.sha256(path.read_bytes()).hexdigest() == metrics['candidate_sha256']
    sample = train.load_gold_partitions(root / 'data/gold', ['2026-10-18'])
    predictions = model.predict(sample[train.FEATURES])
    assert len(predictions) == 245 and set(predictions) <= {0, 1}
    result = {'versions': versions, 'dags': TASKS, 'import_errors': {},
              'raw_hashes_verified': 46, 'saved_model_predictions': len(predictions)}
    if check_gold:
        frame = train.load_gold_partitions(root / 'data/gold')
        _, test, info = train.chronological_split(frame)
        actual = train.evaluate_model(path, test, info)
        assert actual == metrics
        result.update(gold_partitions_validated=46, gold_rows=len(frame), metrics=metrics)
    if baseline:
        original = json.loads(Path(baseline).read_text())
        original = original.get('metrics', original)
        keys = ['accuracy', 'precision', 'recall', 'f1_score']
        for key in keys:
            assert np.isclose(metrics[key], original[key], rtol=0, atol=1e-12), key
        for key in ['confusion_matrix', 'train_rows', 'test_rows', 'train_date_range', 'test_date_range', 'selected_features']:
            assert metrics[key] == original[key], key
        result['phase4_metrics_match'] = True
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check-gold', action='store_true')
    parser.add_argument('--baseline', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(check_gold=args.check_gold, baseline=args.baseline), indent=2))
