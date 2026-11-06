"""Reproducible chronological Gold training; all learned transforms stay in Pipeline."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import logging
from pathlib import Path
import uuid

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src import etl
from scripts.generate_gold import supplied_dates

ROOT = etl.ROOT
NUMERIC_FEATURES = etl.NUMERIC + ['is_weekend', 'distance_per_promised_day']
CATEGORICAL_FEATURES = ['customer_region', 'shipping_method', 'weather_risk_level']
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = 'late_delivery'
log = logging.getLogger(__name__)


def load_gold_partitions(gold_dir=None, dates=None):
    """Require every requested partition (default all supplied dates), never silently skip."""
    gold_dir = Path(gold_dir) if gold_dir is not None else ROOT / 'data/gold'
    dates = supplied_dates() if dates is None else list(dates)
    if not dates or len(set(dates)) != len(dates):
        raise ValueError('Select nonempty, distinct partition dates')
    frames = []
    for day in sorted(etl.checked_date(d) for d in dates):
        path = gold_dir / f'orders_{day}.csv'
        frame = etl.read_frame(path)
        etl.validate_gold_frame(frame, day)
        frames.append(frame)
        log.info('validated date=%s rows=%s path=%s', day, len(frame), path)
    combined = pd.concat(frames, ignore_index=True)
    if combined.order_id.duplicated().any():
        raise ValueError('Duplicate order_id across Gold partitions/dates')
    for column in NUMERIC_FEATURES + [TARGET]:
        combined[column] = pd.to_numeric(combined[column]).astype(float)
    for column in CATEGORICAL_FEATURES:
        combined[column] = combined[column].astype(object).where(combined[column].notna(), np.nan)
    return combined


def chronological_split(frame, train_fraction=0.8):
    if not 0 < train_fraction < 1:
        raise ValueError('train_fraction must be between 0 and 1')
    missing = set(FEATURES + [TARGET, 'order_date', 'order_id']) - set(frame.columns)
    if missing:
        raise ValueError(f'Missing required columns: {sorted(missing)}')
    validate_selected_frame(frame)
    dates = sorted(frame.order_date.unique())
    if len(dates) < 2:
        raise ValueError('Insufficient data: at least two distinct dates required')
    n_train = max(1, min(len(dates)-1, int(len(dates)*train_fraction)))
    train = frame[frame.order_date.isin(dates[:n_train])].copy()
    test = frame[frame.order_date.isin(dates[n_train:])].copy()
    if train[TARGET].nunique() != 2:
        raise ValueError('Training requires both target classes')
    info = {'split_strategy': 'chronological earliest floor(train_fraction * dates) train; latest dates test',
            'train_fraction': train_fraction, 'cutoff_date': dates[n_train],
            'train_rows': len(train), 'test_rows': len(test),
            'train_date_range': [dates[0], dates[n_train-1]],
            'test_date_range': [dates[n_train], dates[-1]],
            'train_dates': dates[:n_train], 'test_dates': dates[n_train:],
            'target_class_distribution': {
                name: {str(int(k)): int(v) for k, v in subset[TARGET].value_counts().sort_index().items()}
                for name, subset in [('train', train), ('test', test)]}}
    log.info('split=%s', info)
    return train, test, info


def validate_selected_frame(frame):
    missing = set(etl.GOLD_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f'Missing required columns: {sorted(missing)}')
    if frame.empty or frame.order_date.isna().any():
        raise ValueError('Insufficient data or missing dates')
    for day, group in frame.groupby('order_date'):
        etl.validate_gold_frame(group, etl.checked_date(day))
    if frame.order_id.duplicated().any():
        raise ValueError('Duplicate order_id across dates')


def build_pipeline(random_state=42):
    numeric = Pipeline([('imputer', SimpleImputer(strategy='median', keep_empty_features=True)),
                        ('scaler', StandardScaler())])
    categorical = Pipeline([('imputer', SimpleImputer(strategy='most_frequent', keep_empty_features=True)),
                            ('encoder', OneHotEncoder(handle_unknown='ignore'))])
    return Pipeline([('preprocessor', ColumnTransformer([
        ('numeric', numeric, NUMERIC_FEATURES), ('categorical', categorical, CATEGORICAL_FEATURES)], remainder='drop')),
        ('classifier', LogisticRegression(random_state=random_state, max_iter=2000, solver='lbfgs'))])


def train_model(train, candidate_path):
    validate_selected_frame(train)
    if train[TARGET].nunique() != 2:
        raise ValueError("Training requires both target classes")
    model = build_pipeline()
    model.fit(train[FEATURES], train[TARGET].astype(int))
    path = Path(candidate_path)
    etl.atomic_write(path, _joblib_bytes(model))
    log.info('trained rows=%s candidate=%s', len(train), path)
    return str(path)


def _joblib_bytes(value):
    import io
    stream = io.BytesIO()
    joblib.dump(value, stream)
    return stream.getvalue()


def evaluate_model(candidate_path, test, split_info):
    """Load the fitted candidate and predict only: no fit or fit_transform here."""
    validate_selected_frame(test)
    model = joblib.load(candidate_path)
    truth = test[TARGET].astype(int)
    prediction = model.predict(test[FEATURES])
    result = dict(split_info, accuracy=float(accuracy_score(truth, prediction)),
                  precision=float(precision_score(truth, prediction, zero_division=0)),
                  recall=float(recall_score(truth, prediction, zero_division=0)),
                  f1_score=float(f1_score(truth, prediction, zero_division=0)),
                  confusion_matrix=confusion_matrix(truth, prediction, labels=[0, 1]).tolist(),
                  confusion_matrix_labels=[0, 1], model_name='LogisticRegression',
                  selected_features=FEATURES, random_state=42,
                  candidate_sha256=hashlib.sha256(Path(candidate_path).read_bytes()).hexdigest())
    log.info('evaluation=%s candidate=%s', result, candidate_path)
    return result


@contextmanager
def publication_lock(root):
    path = Path(root) / 'data/ml/publication.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def save_model(candidate_path, metrics, root=None):
    root = ROOT if root is None else Path(root)
    content = Path(candidate_path).read_bytes()
    if hashlib.sha256(content).hexdigest() != metrics['candidate_sha256']:
        raise ValueError('Candidate changed after evaluation')
    for key in ['accuracy', 'precision', 'recall', 'f1_score']:
        if not np.isfinite(metrics[key]) or not 0 <= metrics[key] <= 1:
            raise ValueError(f'Invalid evaluation metric: {key}')
    with publication_lock(root):
        etl.atomic_write(root / 'models/delivery_delay_model.joblib', content)
        etl.atomic_write(root / 'metrics/metrics.json', (json.dumps(metrics, indent=2, sort_keys=True)+'\n').encode())
    log.info('published model=%s metrics=%s', root / 'models/delivery_delay_model.joblib', root / 'metrics/metrics.json')
    return str(root / 'models/delivery_delay_model.joblib')


def prepare_run(dates=None, run_id=None):
    frame = load_gold_partitions(dates=dates)
    train, test, info = chronological_split(frame)
    # UUID avoids path traversal and collision even on concurrent runs/retries.
    directory = ROOT / 'data/ml' / uuid.uuid4().hex
    directory.mkdir(parents=True)
    joblib.dump((train, test, info), directory / 'split.joblib')
    log.info('run_id=%s staging=%s split=%s', run_id, directory, info)
    return str(directory)


def train_run(directory):
    directory = Path(directory)
    train, _, _ = joblib.load(directory / 'split.joblib')
    train_model(train, directory / 'candidate.joblib')
    return str(directory)


def evaluate_run(directory):
    directory = Path(directory)
    _, test, info = joblib.load(directory / 'split.joblib')
    result = evaluate_model(directory / 'candidate.joblib', test, info)
    etl.atomic_write(directory / 'evaluation.json', json.dumps(result, indent=2).encode())
    return str(directory)


def publish_run(directory):
    directory = Path(directory)
    return save_model(directory / 'candidate.joblib', json.loads((directory / 'evaluation.json').read_text()))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dates', nargs='+')
    logging.basicConfig(level=logging.INFO)
    print(publish_run(evaluate_run(train_run(prepare_run(parser.parse_args().dates)))))
