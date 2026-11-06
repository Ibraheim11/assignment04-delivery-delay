"""Phase 1 only: verify raw archive bytes and profile every daily batch."""
from pathlib import Path
from zipfile import ZipFile
import hashlib
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

def main():
    dictionary = pd.read_csv(ROOT / 'data_dictionary.csv')
    columns = dictionary.column.tolist()
    expected = {f'data/raw/orders_{d:%Y-%m-%d}.csv' for d in pd.date_range('2026-10-01', '2026-11-15')}
    hashes = {}
    with ZipFile(ROOT / 'Assignment04_46_Daily_CSV_Batches.zip') as archive:
        assert archive.testzip() is None, 'Archive CRC failure'
        names = [n for n in archive.namelist() if n.endswith('.csv') and n.startswith('data/raw/')]
        assert len(names) == 46 and set(names) == expected, 'Unexpected batch inventory'
        assert archive.read('data_dictionary.csv') == (ROOT / 'data_dictionary.csv').read_bytes()
        for name in sorted(names):
            content = archive.read(name)
            path = ROOT / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                assert path.read_bytes() == content, f'Refusing to overwrite changed raw file: {name}'
            else:
                path.write_bytes(content)
            hashes[name] = hashlib.sha256(content).hexdigest()
    assert {str(p.relative_to(ROOT)) for p in (ROOT / 'data/raw').glob('*.csv')} == expected
    batches, frames = [], []
    for name in sorted(names):
        frame = pd.read_csv(ROOT / name)
        assert frame.columns.tolist() == columns and not frame.empty, name
        date = Path(name).stem.removeprefix('orders_')
        batches.append({'date': date, 'rows': len(frame), 'duplicate_ids': int(frame.order_id.duplicated().sum()),
                        'exact_duplicates': int(frame.duplicated().sum()), 'missing_cells': int(frame.isna().sum().sum()),
                        'late': int(frame.late_delivery.eq(1).sum()),
                        'wrong_date': int(frame.order_date.ne(date).sum())})
        frames.append(frame)
    df = pd.concat(frames, ignore_index=True)
    numeric = ['order_hour','distance_km','items_count','package_weight_kg','order_value_egp','promised_delivery_days','warehouse_load_pct','carrier_on_time_rate','late_delivery']
    values = {c: pd.to_numeric(df[c], errors='coerce') for c in numeric}
    invalid = {c: int((df[c].notna() & values[c].isna()).sum()) for c in numeric}
    rules = {'order_hour': lambda s: ~s.between(0,23) | s.mod(1).ne(0),
             'distance_km': lambda s:s.le(0), 'items_count':lambda s:s.le(0)|s.mod(1).ne(0),
             'package_weight_kg':lambda s:s.le(0), 'order_value_egp':lambda s:s.le(0),
             'promised_delivery_days':lambda s:s.lt(1)|s.mod(1).ne(0),
             'warehouse_load_pct':lambda s:~s.between(0,100),
             'carrier_on_time_rate':lambda s:~s.between(0,1), 'late_delivery':lambda s:~s.isin([0,1])}
    ranges = {c:int((values[c].notna() & rule(values[c])).sum()) for c,rule in rules.items()}
    categories = {c:{str(k):int(v) for k,v in df[c].value_counts(dropna=False).items()} for c in ['customer_region','warehouse_id','shipping_method','weather_risk_level']}
    summary = {'rows':len(df),'unique_order_ids':int(df.order_id.nunique()), 'duplicate_ids':int(df.order_id.duplicated().sum()),
               'exact_duplicates':int(df.duplicated().sum()), 'missing':df.isna().sum().to_dict(),
               'dtypes':df.dtypes.astype(str).to_dict(), 'numeric_parse_failures':invalid,'invalid_ranges':ranges,
               'categories':categories, 'label_counts':df.late_delivery.value_counts(dropna=False).to_dict(),
               'wrong_batch_dates':sum(b['wrong_date'] for b in batches),
               'numeric_summary':df[numeric].describe().to_dict()}
    out = ROOT / 'reports'
    out.mkdir(exist_ok=True)
    (out/'raw_sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    (out/'exploration_summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    pd.DataFrame(batches).to_csv(out/'batch_profile.csv',index=False,lineterminator='\n')
    lines = ['# Phase 1 data exploration', '', 'Source: all three pages of `Route_Assignment_04_Airflow_Data_ML_Pipelines.pdf` and all 15 rows of `data_dictionary.csv` were read before implementation. Baseline: origin/main `71cffac`. This report profiles raw data without cleaning it.', '',
             '## Reproduction and integrity', '', '`python scripts/explore_data.py` verifies ZIP CRC, the dictionary bytes, exactly 46 consecutive daily files (2026-10-01 through 2026-11-15), nonempty batches, ordered 15-column schemas, and byte equality with the archive. Existing mismatched raw files cause failure instead of being overwritten. SHA-256 hashes are in `raw_sha256.json`. All counts below include raw duplicates.', '',
             '## Observed findings', '', '- Shipping spellings requiring normalization: ` standard ` (185), `EXPRESS` (80), `same_day` (23). Other nonmissing category values match the dictionary.', '- October 8–14 has seven duplicate rows per batch; other dates have three. October 15–21 has 20–34 missing cells per batch, compared with 5–15 on other dates.', '- Example batches: October 1 has 183 rows and 45 late; October 18 has 248 rows and 69 late; November 15 has 252 rows and 66 late. Full per-date counts follow.', '', f'- Rows: {len(df):,}; unique order IDs: {df.order_id.nunique():,}.',
             f'- Duplicate IDs beyond the first occurrence: {df.order_id.duplicated().sum()}; exact duplicate rows: {df.duplicated().sum()}.',
             f'- Rows with dates different from their filename: {summary["wrong_batch_dates"]}.',
             f'- Labels: {summary["label_counts"]}; late rate: {df.late_delivery.mean():.2%}.', '',
             '### Fields, inferred types, missing and invalid values', '', '| Field | Pandas dtype | Missing | Numeric parse failures | Out of range |', '|---|---|---:|---:|---:|']
    for c in columns:
        lines.append(f'| {c} | {df[c].dtype} | {df[c].isna().sum()} | {invalid.get(c,"—")} | {ranges.get(c,"—")} |')
    lines += ['', 'Range checks exclude missing values. Integer fields must be integral; hour 0–23; distance, items, weight and value >0; promise ≥1; carrier rate 0–1; label 0/1. Warehouse load uses an explicit assumed 0–100 percentage range.', '',
              '### Every dated batch', '', '| Date | Rows | Duplicate IDs | Exact duplicates | Missing cells | Late | Wrong date |','|---|---:|---:|---:|---:|---:|---:|']
    for b in batches:
        lines.append('| '+' | '.join(str(v) for v in b.values())+' |')
    lines += ['', '### Raw category frequencies', '', 'Case and whitespace are preserved in the JSON representation below; no raw categories were normalized.', '', '```json',json.dumps(categories,indent=2),'```','',
              '## Field roles and later-phase decisions', '',
              '`late_delivery` is the binary label. `order_id` identifies/deduplicates orders and is excluded from features. `order_date` is the batch/date validation field and supports chronological splitting. `customer_region`, `warehouse_id`, `shipping_method`, and `weather_risk_level` are categorical features. The remaining nine fields (including the label) are numeric; hour, item count and promise are integer-valued.', '',
              'Silver proposal: validate schema, nonempty input, batch dates and binary labels; reject/quarantine invalid identifiers, dates and labels. Deduplicate order_id deterministically, checking conflicting duplicates before choosing a record. Trim/case-normalize categories and map approved spellings to dictionary values; retain missing categories explicitly. Coerce numeric types and replace impossible feature values with missing values, keeping quality counts. Missing positive-valued features must not become zero. Do not estimate imputation statistics in Silver.', '',
              'Training proposal: split chronologically before learning median imputation, category encoding, scaling or any feature selection. Fit those steps on training data only in a Scikit-learn Pipeline. Evaluate on later dates and prevent order IDs crossing the split. Compare class distribution by date because raw label prevalence may shift; aggregate accuracy alone is insufficient.', '',
              'Gold feature candidates for later implementation: order weekday/weekend from order_date and distance per promised day (only with valid denominator); cyclic hour features are another option. All must be available when the order is placed. No post-delivery information is used.', '',
              '## Scope boundary', '', 'Only raw extraction, integrity checks, Pandas profiling, project directories and documentation are implemented. No ETL functions, Airflow DAGs, model training, Docker execution, cleaned datasets, or model/metrics artifacts have been produced. Empty future directories are placeholders. No starter code was present in the fetched repository or archive.']
    (out/'data_exploration.md').write_text('\n'.join(lines)+'\n')
    for name in names:
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest() == hashes[name]
    print(json.dumps({k:summary[k] for k in ['rows','unique_order_ids','duplicate_ids','exact_duplicates','missing','invalid_ranges','label_counts','wrong_batch_dates']},indent=2))
    print('PASS: 46 batches, CRC, dictionary, schema, nonempty data and unchanged raw SHA-256 checks')

if __name__ == '__main__':
    main()
