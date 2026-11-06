"""Date-explicit Medallion ETL. Missing features remain missing for later training."""
import argparse
from datetime import date
import io
import json
import os
from pathlib import Path
import re
import tempfile

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = ['order_id', 'order_date', 'order_hour', 'customer_region', 'warehouse_id',
           'shipping_method', 'distance_km', 'items_count', 'package_weight_kg',
           'order_value_egp', 'promised_delivery_days', 'warehouse_load_pct',
           'weather_risk_level', 'carrier_on_time_rate', 'late_delivery']
CATEGORIES = {'customer_region': ['Cairo', 'Giza', 'Alexandria', 'Delta', 'Upper_Egypt', 'Canal'],
              'warehouse_id': ['WH-01', 'WH-02', 'WH-03', 'WH-04'],
              'shipping_method': ['Standard', 'Express', 'Same_Day'],
              'weather_risk_level': ['Low', 'Medium', 'High']}
INTEGER = ['order_hour', 'items_count', 'promised_delivery_days']
NUMERIC = INTEGER + ['distance_km', 'package_weight_kg', 'order_value_egp',
                     'warehouse_load_pct', 'carrier_on_time_rate']
GOLD_COLUMNS = COLUMNS + ['is_weekend', 'distance_per_promised_day']


def checked_date(batch_date):
    if not isinstance(batch_date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', batch_date):
        raise ValueError('batch_date must be an explicit YYYY-MM-DD string')
    try:
        date.fromisoformat(batch_date)
    except ValueError as exc:
        raise ValueError('Invalid batch_date') from exc
    return batch_date


def path_for(layer, batch_date):
    return ROOT / 'data' / layer / f'orders_{checked_date(batch_date)}.csv'


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_frame(path, frame):
    atomic_write(path, frame.to_csv(index=False, lineterminator='\n', float_format='%.12g').encode())


def read_frame(path):
    try:
        content = path.read_bytes()
        header = pd.read_csv(io.BytesIO(content), nrows=0).columns.tolist()
        if len(header) != len(set(header)) or any(re.search(r'\.\d+$', c) for c in header):
            raise ValueError('Duplicate column names')
        return pd.read_csv(io.BytesIO(content), dtype='string')
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as exc:
        raise ValueError(f'Empty or malformed CSV: {path}') from exc


def validate_base(frame, batch_date, columns=COLUMNS):
    checked_date(batch_date)
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f'Missing required columns: {sorted(missing)}')
    if frame.empty:
        raise ValueError('Empty data')
    if frame.order_id.isna().any() or frame.order_id.str.strip().eq('').any():
        raise ValueError('Missing order_id')
    if not frame.order_date.eq(batch_date).fillna(False).all():
        raise ValueError('Invalid order_date: must equal batch date in YYYY-MM-DD form')
    labels = pd.to_numeric(frame.late_delivery, errors='coerce')
    if not labels.isin([0, 1]).all():
        raise ValueError('Invalid late_delivery: binary labels 0/1 required')


def valid_numeric(series, column):
    valid = series.notna() & np.isfinite(series.astype(float))
    if column in INTEGER:
        valid &= series.mod(1).eq(0)
    if column == 'order_hour':
        valid &= series.between(0, 23)
    elif column == 'warehouse_load_pct':
        valid &= series.between(0, 100)
    elif column == 'carrier_on_time_rate':
        valid &= series.between(0, 1)
    else:
        valid &= series.gt(0)
    return valid.fillna(False)


def extract_to_bronze(batch_date):
    source = path_for('raw', batch_date)
    target = path_for('bronze', batch_date)
    atomic_write(target, source.read_bytes())
    return target


def transform_to_silver(batch_date):
    frame = read_frame(path_for('bronze', batch_date))
    validate_base(frame, batch_date)
    frame = frame[COLUMNS].copy()
    input_rows = len(frame)
    missing = frame.isna().sum().astype(int).to_dict()
    unique = frame.drop_duplicates()
    if unique.order_id.duplicated().any():
        raise ValueError('Conflicting duplicate order_id records')
    frame = unique.copy()
    quality = {'batch_date': batch_date, 'input_rows': input_rows,
               'exact_duplicates_removed': input_rows-len(frame), 'output_rows': len(frame),
               'input_missing': missing, 'invalid_numeric_to_missing': {},
               'unknown_categories_to_missing': {}, 'normalized_categories': {}}
    for column, approved in CATEGORIES.items():
        original = frame[column]
        normalized = original.str.strip().str.casefold().map({v.casefold(): v for v in approved})
        quality['unknown_categories_to_missing'][column] = int((original.notna() & normalized.isna()).sum())
        quality['normalized_categories'][column] = int((original.notna() & normalized.notna() & original.ne(normalized)).sum())
        frame[column] = normalized
    for column in NUMERIC:
        original = frame[column]
        numeric = pd.to_numeric(original, errors='coerce').astype('Float64')
        valid = valid_numeric(numeric, column)
        quality['invalid_numeric_to_missing'][column] = int((original.notna() & ~valid).sum())
        frame[column] = numeric.where(valid).astype('Int64' if column in INTEGER else 'Float64')
    frame['late_delivery'] = pd.to_numeric(frame.late_delivery).astype('Int64')
    frame = frame.sort_values('order_id', kind='stable').reset_index(drop=True)
    quality['output_missing'] = frame.isna().sum().astype(int).to_dict()
    target = path_for('silver', batch_date)
    write_frame(target, frame)
    atomic_write(ROOT / 'reports' / 'etl' / f'{batch_date}.json', (json.dumps(quality, indent=2, sort_keys=True)+'\n').encode())
    return target


def prepare_gold(batch_date):
    frame = read_frame(path_for('silver', batch_date))
    validate_base(frame, batch_date)
    frame = frame[COLUMNS].copy()
    frame['is_weekend'] = int(date.fromisoformat(batch_date).weekday() >= 5)
    distance = pd.to_numeric(frame.distance_km, errors='coerce')
    promise = pd.to_numeric(frame.promised_delivery_days, errors='coerce')
    frame['distance_per_promised_day'] = distance / promise.where(promise.ge(1))
    validate_gold_frame(frame, batch_date)
    target = path_for('gold', batch_date)
    write_frame(target, frame)
    return target


def validate_gold_frame(frame, batch_date):
    validate_base(frame, batch_date, GOLD_COLUMNS)
    if frame.order_id.duplicated().any():
        raise ValueError('Duplicate order_id in Gold')
    for column, approved in CATEGORIES.items():
        if (frame[column].notna() & ~frame[column].isin(approved)).any():
            raise ValueError(f'Invalid category: {column}')
    for column in NUMERIC:
        numeric = pd.to_numeric(frame[column], errors='coerce').astype('Float64')
        if (frame[column].notna() & ~valid_numeric(numeric, column)).any():
            raise ValueError(f'Invalid numeric feature: {column}')
    weekend = pd.to_numeric(frame.is_weekend, errors='coerce')
    if not weekend.eq(int(date.fromisoformat(batch_date).weekday() >= 5)).fillna(False).all():
        raise ValueError('Invalid is_weekend')
    expected = pd.to_numeric(frame.distance_km, errors='coerce') / pd.to_numeric(frame.promised_delivery_days, errors='coerce')
    actual = pd.to_numeric(frame.distance_per_promised_day, errors='coerce')
    if (frame.distance_per_promised_day.notna() & actual.isna()).any() or not np.allclose(actual.astype(float), expected.astype(float), rtol=1e-10, atol=1e-10, equal_nan=True):
        raise ValueError('Invalid distance_per_promised_day')


def validate_gold(batch_date):
    frame = read_frame(path_for('gold', batch_date))
    validate_gold_frame(frame, batch_date)
    return {'batch_date': batch_date, 'rows': len(frame), 'duplicate_ids': 0}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('batch_date')
    args = parser.parse_args()
    extract_to_bronze(args.batch_date)
    transform_to_silver(args.batch_date)
    prepare_gold(args.batch_date)
    print(json.dumps(validate_gold(args.batch_date), sort_keys=True))
