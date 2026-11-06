"""Gold validation, chronology, fitted-transform isolation and publication contracts."""
import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.pipeline import Pipeline

from src import etl, train
from scripts.generate_gold import generate

REPO = etl.ROOT
DATES = ['2026-10-01', '2026-10-18', '2026-10-19', '2026-10-20']


@pytest.fixture
def gold(tmp_path):
    for day in DATES:
        source = REPO / 'data/gold' / f'orders_{day}.csv'
        (tmp_path / source.name).write_bytes(source.read_bytes())
    return tmp_path


def test_gold_loading(gold):
    frame = train.load_gold_partitions(gold, DATES)
    assert len(frame) == 798
    assert not frame.order_id.duplicated().any()
    assert set(frame.order_date) == set(DATES)
    assert frame.distance_km.dtype == float
    with pytest.raises(FileNotFoundError):
        train.load_gold_partitions(gold)  # default requires all 46, not available subset
    for dates in [[], [DATES[0], DATES[0]], ['../bad']]:
        with pytest.raises(ValueError): train.load_gold_partitions(gold, dates)


@pytest.mark.parametrize('column,value', [('order_hour', 24), ('items_count', 1.5),
    ('distance_km', -1), ('carrier_on_time_rate', 2), ('shipping_method', 'invalid'),
    ('late_delivery', 2), ('late_delivery', None), ('is_weekend', 1),
    ('distance_per_promised_day', 'bad'), ('order_date', '2026-10-02')])
def test_invalid_gold(gold, column, value):
    path = gold / f'orders_{DATES[0]}.csv'
    frame = pd.read_csv(path).astype(object)
    frame.loc[0, column] = value
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError): train.load_gold_partitions(gold, DATES)


@pytest.mark.parametrize('column', train.FEATURES + ['late_delivery', 'order_id'])
def test_missing_columns(gold, column):
    path = gold / f'orders_{DATES[0]}.csv'
    pd.read_csv(path).drop(columns=column).to_csv(path, index=False)
    with pytest.raises(ValueError, match='Missing required'): train.load_gold_partitions(gold, DATES)


def test_cross_date_duplicates(gold):
    first = pd.read_csv(gold / f'orders_{DATES[0]}.csv').order_id.iloc[0]
    path = gold / f'orders_{DATES[1]}.csv'
    frame = pd.read_csv(path)
    frame.loc[0, 'order_id'] = first
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError, match='across Gold'): train.load_gold_partitions(gold, DATES)


def test_chronology_and_insufficient(gold):
    frame = train.load_gold_partitions(gold, DATES)
    a, b, info = train.chronological_split(frame.sample(frac=1, random_state=7))
    assert a.order_date.max() < b.order_date.min()
    assert set(a.order_id).isdisjoint(b.order_id)
    assert info['train_rows'] == 605 and info['test_rows'] == 193
    assert info['cutoff_date'] == '2026-10-20'
    for invalid in [frame.iloc[:0], frame[frame.order_date.eq(DATES[0])]]:
        with pytest.raises(ValueError, match='Insufficient'): train.chronological_split(invalid)
    frame.late_delivery = 0
    with pytest.raises(ValueError, match='both target classes'): train.chronological_split(frame)


def test_fit_training_only_and_serialization(gold, tmp_path, monkeypatch):
    a, b, info = train.chronological_split(train.load_gold_partitions(gold, DATES))
    # A held-out outlier and unseen category cannot affect fitted statistics/vocabulary.
    b.loc[:, 'order_value_egp'] = 1e10
    a.loc[:, 'customer_region'] = 'Cairo'
    b.loc[:, 'customer_region'] = 'Giza'
    path = tmp_path / 'candidate.joblib'
    train.train_model(a, path)
    model = joblib.load(path)
    assert isinstance(model, Pipeline)
    pre = model.named_steps['preprocessor']
    imputer = pre.named_transformers_['numeric'].named_steps['imputer']
    np.testing.assert_allclose(imputer.statistics_, a[train.NUMERIC_FEATURES].median())
    encoded = pre.named_transformers_['categorical'].named_steps['encoder']
    assert 'Giza' not in encoded.categories_[0]
    scaler = pre.named_transformers_['numeric'].named_steps['scaler']
    expected = a[train.NUMERIC_FEATURES].fillna(a[train.NUMERIC_FEATURES].median()).mean()
    np.testing.assert_allclose(scaler.mean_, expected)
    before = path.read_bytes()
    def forbidden(*args, **kwargs): raise AssertionError('Evaluation must never fit')
    monkeypatch.setattr(Pipeline, 'fit', forbidden)
    metrics = train.evaluate_model(path, b, info)
    assert path.read_bytes() == before
    assert 0 <= metrics['f1_score'] <= 1
    assert sum(map(sum, metrics['confusion_matrix'])) == len(b)
    train.save_model(path, metrics, tmp_path)
    saved = joblib.load(tmp_path / 'models/delivery_delay_model.joblib')
    np.testing.assert_array_equal(saved.predict(b[train.FEATURES]), model.predict(b[train.FEATURES]))
    assert json.loads((tmp_path / 'metrics/metrics.json').read_text()) == metrics
    path.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='changed after evaluation'): train.save_model(path, metrics, tmp_path)
    assert (tmp_path / 'models/delivery_delay_model.joblib').read_bytes() == before


def test_repeatability_and_no_early_publication(gold, tmp_path, monkeypatch):
    monkeypatch.setattr(train, 'ROOT', tmp_path)
    (tmp_path / 'data/gold').mkdir(parents=True)
    for path in gold.glob('*.csv'):
        (tmp_path / 'data/gold' / path.name).write_bytes(path.read_bytes())
    one, two = train.prepare_run(DATES, 'same'), train.prepare_run(DATES, 'same')
    assert one != two
    assert not (tmp_path / 'models/delivery_delay_model.joblib').exists()
    for directory in [one, two]:
        train.train_run(directory)
        if directory == one:
            assert not (tmp_path / 'models/delivery_delay_model.joblib').exists()
        with pytest.raises(FileNotFoundError): train.publish_run(directory)
        train.evaluate_run(directory)
        train.publish_run(directory)
    assert (Path(one) / 'candidate.joblib').read_bytes() == (Path(two) / 'candidate.joblib').read_bytes()
    assert (Path(one) / 'evaluation.json').read_bytes() == (Path(two) / 'evaluation.json').read_bytes()


def test_generate_uses_existing_etl_and_preserves_raw(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    for day in DATES:
        path = etl.path_for('raw', day)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((REPO / 'data/raw' / path.name).read_bytes())
    raw = {p: p.read_bytes() for p in (tmp_path / 'data/raw').glob('*.csv')}
    assert sum(x['rows'] for x in generate(DATES)) == 798
    before = {p: p.read_bytes() for p in (tmp_path / 'data/gold').glob('*.csv')}
    generate(DATES)
    assert all(p.read_bytes() == content for p, content in {**before, **raw}.items())
