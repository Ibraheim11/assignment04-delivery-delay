from pathlib import Path
import pandas as pd
import pytest
from src import etl

DATE = '2026-10-01'
REPO = etl.ROOT

@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, 'ROOT', tmp_path)
    for day in [DATE, '2026-10-18']:
        target = etl.path_for('raw', day)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REPO/'data/raw'/target.name).read_bytes())
    return tmp_path


def run(day=DATE):
    etl.extract_to_bronze(day)
    etl.transform_to_silver(day)
    etl.prepare_gold(day)
    return etl.validate_gold(day)


def test_bytes_isolation_idempotence(root):
    source = etl.path_for('raw', DATE).read_bytes()
    assert run()['rows'] == 180
    outputs = {layer: etl.path_for(layer, DATE).read_bytes() for layer in ['bronze','silver','gold']}
    assert outputs['bronze'] == source
    run('2026-10-18')
    assert run()['rows'] == 180
    for layer, content in outputs.items():
        assert etl.path_for(layer, DATE).read_bytes() == content
    assert etl.path_for('raw', DATE).read_bytes() == source
    assert not pd.read_csv(etl.path_for('gold', DATE)).order_id.duplicated().any()


def test_cleaning_and_features(root):
    frame = pd.read_csv(etl.path_for('raw', DATE)).iloc[:2].copy()
    frame.loc[0, ['shipping_method','distance_km','items_count']] = [' EXPRESS ', -1, 0]
    frame.loc[1, ['shipping_method','order_hour','promised_delivery_days']] = ['unknown', 24, 0]
    frame.to_csv(etl.path_for('raw', DATE), index=False)
    run()
    silver = pd.read_csv(etl.path_for('silver', DATE)).set_index('order_id')
    a = silver.loc[frame.loc[0,'order_id']]
    b = silver.loc[frame.loc[1,'order_id']]
    assert a.shipping_method == 'Express'
    assert pd.isna(a.distance_km) and pd.isna(a.items_count)
    assert pd.isna(b.shipping_method) and pd.isna(b.order_hour)
    gold = pd.read_csv(etl.path_for('gold', DATE))
    assert gold.is_weekend.eq(0).all()
    assert gold.distance_per_promised_day.isna().all()
    assert gold.late_delivery.tolist() == silver.late_delivery.tolist()


@pytest.mark.parametrize('kind,message', [('missing','Missing required'), ('empty','Empty'),
    ('date','Invalid order_date'), ('label','Invalid late_delivery'), ('conflict','Conflicting duplicate'), ('id','Missing order_id')])
def test_silver_errors(root, kind, message):
    etl.extract_to_bronze(DATE)
    path = etl.path_for('bronze', DATE)
    frame = pd.read_csv(path)
    if kind == 'missing': frame = frame.drop(columns='distance_km')
    if kind == 'empty': frame = frame.iloc[:0]
    if kind == 'date': frame.loc[0,'order_date'] = '2026-02-30'
    if kind == 'label': frame.loc[0,'late_delivery'] = 2
    if kind == 'id': frame.loc[0,'order_id'] = None
    if kind == 'conflict':
        other = frame.iloc[[0]].copy(); other['distance_km'] = 999
        frame = pd.concat([frame,other])
    frame.to_csv(path,index=False)
    with pytest.raises(ValueError, match=message): etl.transform_to_silver(DATE)
    assert not etl.path_for('silver', DATE).exists()


@pytest.mark.parametrize('kind,message', [('duplicate','Duplicate order_id'), ('date','Invalid order_date'),
    ('label','Invalid late_delivery'), ('weekend','Invalid is_weekend'), ('ratio','Invalid distance_per_promised_day'),
    ('missing','Missing required'), ('empty','Empty'), ('numeric','Invalid numeric')])
def test_gold_errors(root,kind,message):
    run()
    path = etl.path_for('gold', DATE)
    frame = pd.read_csv(path)
    if kind == 'duplicate': frame = pd.concat([frame,frame.iloc[[0]]])
    if kind == 'date': frame.loc[0,'order_date'] = '2026-10-02'
    if kind == 'label': frame.loc[0,'late_delivery'] = None
    if kind == 'weekend': frame.loc[0,'is_weekend'] = 1
    if kind == 'ratio': frame.loc[0,'distance_per_promised_day'] = -1
    if kind == 'missing': frame = frame.drop(columns='is_weekend')
    if kind == 'empty': frame = frame.iloc[:0]
    if kind == 'numeric': frame.loc[0,'items_count'] = 1.5
    frame.to_csv(path,index=False)
    with pytest.raises(ValueError,match=message): etl.validate_gold(DATE)


@pytest.mark.parametrize('value', [None,'','2026-2-01','2026-02-30','../orders'])
def test_explicit_dates(root,value):
    with pytest.raises(ValueError): etl.extract_to_bronze(value)


def test_missing_source(root):
    with pytest.raises(FileNotFoundError): etl.extract_to_bronze('2026-12-01')


def test_failed_atomic_replace_preserves_existing(root,monkeypatch):
    path = root/'output.csv'; path.write_bytes(b'old')
    def fail(*args): raise OSError('simulated replace failure')
    monkeypatch.setattr(etl.os,'replace',fail)
    with pytest.raises(OSError): etl.atomic_write(path,b'new')
    assert path.read_bytes()==b'old'
    assert list(root.glob('tmp*'))==[]
