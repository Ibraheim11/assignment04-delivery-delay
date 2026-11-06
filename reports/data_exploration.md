# Phase 1 data exploration

Source: all three pages of `Route_Assignment_04_Airflow_Data_ML_Pipelines.pdf` and all 15 rows of `data_dictionary.csv` were read before implementation. Baseline: origin/main `71cffac`. This report profiles raw data without cleaning it.

## Reproduction and integrity

`python scripts/explore_data.py` verifies ZIP CRC, the dictionary bytes, exactly 46 consecutive daily files (2026-10-01 through 2026-11-15), nonempty batches, ordered 15-column schemas, and byte equality with the archive. Existing mismatched raw files cause failure instead of being overwritten. SHA-256 hashes are in `raw_sha256.json`. All counts below include raw duplicates.

## Observed findings

- Shipping spellings requiring normalization: ` standard ` (185), `EXPRESS` (80), `same_day` (23). Other nonmissing category values match the dictionary.
- October 8–14 has seven duplicate rows per batch; other dates have three. October 15–21 has 20–34 missing cells per batch, compared with 5–15 on other dates.
- Example batches: October 1 has 183 rows and 45 late; October 18 has 248 rows and 69 late; November 15 has 252 rows and 66 late. Full per-date counts follow.

- Rows: 10,219; unique order IDs: 10,053.
- Duplicate IDs beyond the first occurrence: 166; exact duplicate rows: 166.
- Rows with dates different from their filename: 0.
- Labels: {0: 7800, 1: 2419}; late rate: 23.67%.

### Fields, inferred types, missing and invalid values

| Field | Pandas dtype | Missing | Numeric parse failures | Out of range |
|---|---|---:|---:|---:|
| order_id | object | 0 | — | — |
| order_date | object | 0 | — | — |
| order_hour | int64 | 0 | 0 | 0 |
| customer_region | object | 0 | — | — |
| warehouse_id | object | 0 | — | — |
| shipping_method | object | 74 | — | — |
| distance_km | float64 | 142 | 0 | 81 |
| items_count | int64 | 0 | 0 | 98 |
| package_weight_kg | float64 | 0 | 0 | 0 |
| order_value_egp | float64 | 0 | 0 | 55 |
| promised_delivery_days | int64 | 0 | 0 | 0 |
| warehouse_load_pct | float64 | 149 | 0 | 0 |
| weather_risk_level | object | 94 | — | — |
| carrier_on_time_rate | float64 | 87 | 0 | 0 |
| late_delivery | int64 | 0 | 0 | 0 |

Range checks exclude missing values. Integer fields must be integral; hour 0–23; distance, items, weight and value >0; promise ≥1; carrier rate 0–1; label 0/1. Warehouse load uses an explicit assumed 0–100 percentage range.

### Every dated batch

| Date | Rows | Duplicate IDs | Exact duplicates | Missing cells | Late | Wrong date |
|---|---:|---:|---:|---:|---:|---:|
| 2026-10-01 | 183 | 3 | 3 | 9 | 45 | 0 |
| 2026-10-02 | 196 | 3 | 3 | 6 | 41 | 0 |
| 2026-10-03 | 209 | 3 | 3 | 8 | 45 | 0 |
| 2026-10-04 | 222 | 3 | 3 | 11 | 50 | 0 |
| 2026-10-05 | 235 | 3 | 3 | 8 | 51 | 0 |
| 2026-10-06 | 248 | 3 | 3 | 10 | 49 | 0 |
| 2026-10-07 | 183 | 3 | 3 | 13 | 47 | 0 |
| 2026-10-08 | 200 | 7 | 7 | 7 | 36 | 0 |
| 2026-10-09 | 213 | 7 | 7 | 5 | 57 | 0 |
| 2026-10-10 | 226 | 7 | 7 | 5 | 52 | 0 |
| 2026-10-11 | 239 | 7 | 7 | 12 | 48 | 0 |
| 2026-10-12 | 252 | 7 | 7 | 7 | 58 | 0 |
| 2026-10-13 | 187 | 7 | 7 | 5 | 37 | 0 |
| 2026-10-14 | 200 | 7 | 7 | 10 | 38 | 0 |
| 2026-10-15 | 209 | 3 | 3 | 30 | 55 | 0 |
| 2026-10-16 | 222 | 3 | 3 | 31 | 57 | 0 |
| 2026-10-17 | 235 | 3 | 3 | 34 | 58 | 0 |
| 2026-10-18 | 248 | 3 | 3 | 29 | 69 | 0 |
| 2026-10-19 | 183 | 3 | 3 | 20 | 48 | 0 |
| 2026-10-20 | 196 | 3 | 3 | 26 | 28 | 0 |
| 2026-10-21 | 209 | 3 | 3 | 25 | 47 | 0 |
| 2026-10-22 | 222 | 3 | 3 | 9 | 47 | 0 |
| 2026-10-23 | 235 | 3 | 3 | 9 | 44 | 0 |
| 2026-10-24 | 248 | 3 | 3 | 12 | 56 | 0 |
| 2026-10-25 | 183 | 3 | 3 | 9 | 39 | 0 |
| 2026-10-26 | 196 | 3 | 3 | 7 | 46 | 0 |
| 2026-10-27 | 209 | 3 | 3 | 6 | 45 | 0 |
| 2026-10-28 | 222 | 3 | 3 | 6 | 53 | 0 |
| 2026-10-29 | 235 | 3 | 3 | 11 | 46 | 0 |
| 2026-10-30 | 248 | 3 | 3 | 9 | 46 | 0 |
| 2026-10-31 | 183 | 3 | 3 | 5 | 33 | 0 |
| 2026-11-01 | 196 | 3 | 3 | 5 | 39 | 0 |
| 2026-11-02 | 209 | 3 | 3 | 12 | 39 | 0 |
| 2026-11-03 | 222 | 3 | 3 | 9 | 46 | 0 |
| 2026-11-04 | 235 | 3 | 3 | 6 | 43 | 0 |
| 2026-11-05 | 278 | 3 | 3 | 14 | 76 | 0 |
| 2026-11-06 | 213 | 3 | 3 | 15 | 73 | 0 |
| 2026-11-07 | 226 | 3 | 3 | 7 | 75 | 0 |
| 2026-11-08 | 239 | 3 | 3 | 11 | 60 | 0 |
| 2026-11-09 | 252 | 3 | 3 | 8 | 81 | 0 |
| 2026-11-10 | 265 | 3 | 3 | 10 | 80 | 0 |
| 2026-11-11 | 278 | 3 | 3 | 9 | 79 | 0 |
| 2026-11-12 | 213 | 3 | 3 | 9 | 61 | 0 |
| 2026-11-13 | 226 | 3 | 3 | 13 | 58 | 0 |
| 2026-11-14 | 239 | 3 | 3 | 13 | 72 | 0 |
| 2026-11-15 | 252 | 3 | 3 | 11 | 66 | 0 |

### Raw category frequencies

Case and whitespace are preserved in the JSON representation below; no raw categories were normalized.

```json
{
  "customer_region": {
    "Cairo": 2885,
    "Giza": 1991,
    "Delta": 1919,
    "Alexandria": 1690,
    "Upper_Egypt": 1016,
    "Canal": 718
  },
  "warehouse_id": {
    "WH-04": 2632,
    "WH-03": 2546,
    "WH-01": 2545,
    "WH-02": 2496
  },
  "shipping_method": {
    "Standard": 5907,
    "Express": 2974,
    "Same_Day": 976,
    " standard ": 185,
    "EXPRESS": 80,
    "nan": 74,
    "same_day": 23
  },
  "weather_risk_level": {
    "Low": 5165,
    "Medium": 3436,
    "High": 1524,
    "nan": 94
  }
}
```

## Field roles and later-phase decisions

`late_delivery` is the binary label. `order_id` identifies/deduplicates orders and is excluded from features. `order_date` is the batch/date validation field and supports chronological splitting. `customer_region`, `warehouse_id`, `shipping_method`, and `weather_risk_level` are categorical features. The remaining nine fields (including the label) are numeric; hour, item count and promise are integer-valued.

Silver proposal: validate schema, nonempty input, batch dates and binary labels; reject/quarantine invalid identifiers, dates and labels. Deduplicate order_id deterministically, checking conflicting duplicates before choosing a record. Trim/case-normalize categories and map approved spellings to dictionary values; retain missing categories explicitly. Coerce numeric types and replace impossible feature values with missing values, keeping quality counts. Missing positive-valued features must not become zero. Do not estimate imputation statistics in Silver.

Training proposal: split chronologically before learning median imputation, category encoding, scaling or any feature selection. Fit those steps on training data only in a Scikit-learn Pipeline. Evaluate on later dates and prevent order IDs crossing the split. Compare class distribution by date because raw label prevalence may shift; aggregate accuracy alone is insufficient.

Gold feature candidates for later implementation: order weekday/weekend from order_date and distance per promised day (only with valid denominator); cyclic hour features are another option. All must be available when the order is placed. No post-delivery information is used.

## Scope boundary

Only raw extraction, integrity checks, Pandas profiling, project directories and documentation are implemented. No ETL functions, Airflow DAGs, model training, Docker execution, cleaned datasets, or model/metrics artifacts have been produced. Empty future directories are placeholders. No starter code was present in the fetched repository or archive.
