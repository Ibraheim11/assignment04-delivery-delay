# Delivery delay prediction — Assignment 04

Phases 1 and 2 are complete. Phase 1: the supplied daily delivery orders are extracted unchanged,
verified, and explored with Pandas. The target is `late_delivery` (0 on time, 1 late).
All three pages of the assignment PDF and the complete data dictionary were read
before implementation. The PDF is present in fetched `origin/main` at `71cffac`.

## Reproduce Phase 1

Use Python 3.12:

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/explore_data.py
```

The script safely extracts absent raw files, refuses to overwrite different existing
files, verifies archive CRC and dictionary equality, checks the exact 46-day inventory,
nonempty batches and all 15 schema columns, then profiles all batches. It verifies
SHA-256 hashes against archive bytes before and after exploration. No cleaning is
applied to raw CSVs. Repeated runs regenerate the same reports.

## Actual findings

- 46 CSVs cover October 1–November 15, 2026, with 10,219 rows and 10,053 unique orders.
- There are 166 repeated IDs and 166 exact duplicate rows. October 8–14 has seven
  duplicates per batch; other dates have three.
- Labels are 7,800 on time and 2,419 late (23.67% late); no invalid labels or
  filename/order-date mismatches were found.
- Missing values: shipping method 74, distance 142, warehouse load 149,
  weather risk 94 and carrier reliability 87 (546 missing cells in five fields).
- Invalid feature values: 81 nonpositive distances, 98 nonpositive item counts,
  and 55 nonpositive order values. Other checked numeric ranges pass.
- Shipping spellings include ` standard ` (185), `EXPRESS` (80), and `same_day` (23).
- October 15–21 has elevated missingness (20–34 cells per batch).

See [the full exploration report](reports/data_exploration.md) for all 46 dated
profiles, inferred types, category frequencies, validation definitions and proposed
cleaning rules. Machine-readable evidence is in
[exploration_summary.json](reports/exploration_summary.json),
[batch_profile.csv](reports/batch_profile.csv), and
[raw_sha256.json](reports/raw_sha256.json).

## Phase 2 Medallion ETL

`src/etl.py` exposes `extract_to_bronze(batch_date)`,
`transform_to_silver(batch_date)`, `prepare_gold(batch_date)` and
`validate_gold(batch_date)`. Each requires a strict, explicit ISO date. Paths are
`data/{layer}/orders_YYYY-MM-DD.csv`; no system-date fallback or glob selection.
Files are replaced atomically using a temporary file in the destination directory.

### Cleaning rules

- Bronze is a byte-for-byte copy, including duplicates and CRLF endings.
- Silver rejects empty inputs, missing required columns, blank IDs, incorrect batch
  dates and nonbinary/missing labels. Exact repeated records are removed before
  cleaning. Any remaining repeated ID fails as a conflicting record. Keep the first
  exact record and sort output by order_id. No valid unique orders are discarded.
- Categories are trimmed and case-normalized to dictionary spellings. Missing or
  unapproved values become empty CSV cells (missing), never guessed categories.
- Numeric parse failures, infinities and out-of-range values become missing.
  Hour must be integral 0–23; items and promise integral ≥1; distance, weight and
  value positive; carrier rate 0–1. Warehouse load is assumed to be 0–100 percent.
  Integer features use nullable Int64; continuous features use nullable Float64.
  Target labels use Int64. No imputer, encoder or scaler is fitted.
- Per-date `reports/etl/YYYY-MM-DD.json` records input/output counts, duplicates
  removed, missing cells, category normalization and invalid-to-missing counts.
  Input missing counts include duplicates; conversion counts apply after deduplication.

### Gold features and validation

Gold preserves the 15 dictionary fields and adds `is_weekend` (Saturday/Sunday = 1)
and `distance_per_promised_day` (valid distance divided by valid promise; missing
when either is unavailable). The weekend definition is a documented calendar
convention rather than a local business-calendar assumption. Features are available
at order placement; no post-delivery measurements are used. `order_id` and
`order_date` are retained for validation and future chronological splitting, but
IDs must be excluded from future model inputs. Target is retained separately.

Gold validation rejects empty/missing-schema data, duplicate or missing IDs,
wrong dates, invalid labels, noncanonical categories, invalid numeric features,
incorrect weekend flags and ratios inconsistent with their inputs (including
missingness). CSV floats use 12 significant digits; ratio checks tolerate that
serialization precision. Learned preprocessing and train/test splitting remain
future work.

### Run and verify

From the repository root, with Python 3.12:

```sh
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m src.etl 2026-10-01
.venv/bin/python -m scripts.verify_etl
.venv/bin/python -m pytest -q
```

Verification executes October 1, 18, 19 and 20 twice each, checks all 46 raw hashes,
Bronze byte equality, no Gold duplicate IDs, stable contents and row counts.
Actual evidence is committed in `reports/etl_verification.json`.

| Batch date | Bronze rows | Silver rows | Gold rows | Duplicates removed |
|---|---:|---:|---:|---:|
| 2026-10-01 | 183 | 180 | 180 | 3 |
| 2026-10-18 | 248 | 245 | 245 | 3 |
| 2026-10-19 | 183 | 180 | 180 | 3 |
| 2026-10-20 | 196 | 193 | 193 | 3 |

Tests: **23 passed**, covering transformations, validation failures, source byte
preservation, date isolation, identical reruns, explicit-date errors, missing source
and atomic-write failure behavior. Tests use temporary directories, never production
raw files. Phase 1 missingness was verified from its script and JSON: the correct
sum is **546**, replacing the README typo of 646.

### Limitations and scope

Only four example partitions are generated; the remaining source batches are
preserved for later runs. Unknown feature values remain missing for future
training-only preprocessing. Atomic replacement protects individual files, not an
entire multi-stage run; downstream steps must stop on failure. Concurrent writes
to the same date are not coordinated. Quality reports describe successful Silver
runs, not an orchestration audit history. Extra input columns are ignored after
required-column validation. Empty future directories remain placeholders.

No Airflow DAGs, ML training, Docker execution or screenshots were added. Nothing
was merged into main. The Phase 1 exploration report describes that earlier phase;
Phase 2 evidence is in the ETL reports. No model or evaluation metrics exist yet.
