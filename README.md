# Delivery delay prediction — Assignment 04

Phases 1–4 are complete. Phase 1: the supplied daily delivery orders are extracted unchanged,
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
entire multi-stage run; downstream steps must stop on failure. Direct ETL CLI writes to the same date are not coordinated; the Phase 3 DAG
adds concurrency protection as documented below. Quality reports describe successful Silver
runs, not an orchestration audit history. Extra input columns are ignored after
required-column validation. Empty future directories remain placeholders.

At the end of Phase 2, no Airflow DAGs, ML training, Docker execution or screenshots had been added. Nothing
was merged into main. The Phase 1 exploration report describes that earlier phase;
Phase 2 evidence is in the ETL reports. No model or evaluation metrics exist yet.

## Phase 3 — Airflow data DAG

`dags/data_pipeline.py` uses the public Apache Airflow 3.1 `airflow.sdk`
TaskFlow API. No cleaning logic is duplicated:

```text
extract_to_bronze -> transform_to_silver -> prepare_gold -> validate_gold
```

Each task calls its tested counterpart in `src/etl.py`. Only the validated date
string passes through XCom; DataFrames stay local. The last task returns no data.
Every task uses `all_success`, two retries and a five-minute retry delay. An
exception is logged and raised; dependent tasks cannot run after an exhausted
upstream failure, even if an older partition already exists. Logs include processing
date, source file, output path, row count, quality report path and validation result.

### Scheduling and date handling

- `schedule="@daily"`, UTC, fixed start date October 1, 2026, and `catchup=False`.
  Enabling the DAG does not automatically replay all historical dates.
- For daily interval timetables, `[2026-10-18, 2026-10-19)` processes October 18.
- Airflow 3.1 defaults `scheduler.create_cron_data_intervals=False`: `@daily` uses
  a midnight trigger with a zero-length interval. The October 19 midnight trigger
  processes October 18, the day just closed. With that default, an explicit
  historical trigger point of October 1 corresponds to September 30 and will fail
  because that source is absent. Use manual batch dates to process October 1,
  or enable daily data intervals for historical backfills as shown below.
- Both timetable configurations are supported and tested. Non-midnight or
  non-daily automated intervals fail clearly. Automated runs reject a conf date
  override; they always follow their interval. No wall-clock date selects a file.
- Manual runs **require** `dag_run.conf.batch_date` as a strict `YYYY-MM-DD`
  string, independently of their logical date or inferred interval. Missing,
  malformed or impossible dates fail in extraction. One run selects exactly
  `data/raw/orders_YYYY-MM-DD.csv`; there is no glob, latest-file or date fallback.
- The supplied final batch, November 15, is processed by the November 16 midnight
  scheduled run. Later scheduled dates have no supplied file: extraction raises
  `FileNotFoundError`, retries twice, then fails and blocks downstream tasks. The
  operator must supply the exact missing batch or pause the DAG; it never reuses
  November 15 silently. Missing dates within the supplied range fail identically.

### Idempotence and concurrency

The existing ETL replaces date-specific outputs atomically and deterministically.
`max_active_runs=1` serializes ordinary DAG runs. Each task also holds a Linux
`fcntl.flock` lock at `data/.locks/DATE.lock` while reading/writing that partition,
including its quality report. This prevents conflicting simultaneous writes even
from overlapping Airflow backfill jobs. Lock release is automatic on failure or
process exit; persistent lock files must not be deleted while executions run.
All workers must share the repository/data filesystem with working POSIX locks;
raw batches must remain immutable. Direct `python -m src.etl` executions do not
participate in these DAG locks and must not run concurrently with the DAG.
A multi-stage run is not one filesystem transaction; completed upstream outputs
remain after failure, and a rerun safely replaces them. No output is appended.

### Environment and execution commands

Use the already configured Python 3.12 / Airflow 3.1 environment, or install Airflow
into a dedicated virtual environment using its official Python 3.12 constraints:

```sh
python -m pip install 'apache-airflow==3.1.0' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt
python -m pip install -r requirements-dev.txt
export AIRFLOW_HOME="$PWD/work/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db migrate
airflow dags list --local
airflow dags list-import-errors --local
python -m pytest -q
# Opt-in tests create real runs in the development metadata DB, isolated data outputs:
RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q
# One real local DagRun; no scheduler is required:
airflow dags test data_pipeline 2026-10-01 -c '{"batch_date":"2026-10-01"}'
```

For this workspace, first `source /workspace/.delivery-delay/env.sh` to use the
preinstalled environment and existing development metadata DB, then export
`AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"` from the repository root. Do not commit
metadata DBs, environments, authentication files or scratch logs.

To queue a manual run with running Airflow services (for example `airflow standalone`
in a development environment):

```sh
airflow dags trigger data_pipeline --conf '{"batch_date":"2026-10-18"}'
```

Historical processing remains explicit despite `catchup=False`. The least ambiguous
option is three manual triggers with batch dates October 18, 19 and 20. For Airflow
3.1 native backfill, configure **all** Airflow services to use data intervals before
parsing/serializing the DAG, then run:

```sh
export AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=True
# Restart services with this configuration and wait for DAG reserialization.
airflow backfill create --dag-id data_pipeline \
  --from-date 2026-10-18 --to-date 2026-10-20 \
  --max-active-runs 1 --reprocess-behavior completed
```

Those logical intervals select October 18, 19 and 20 respectively. With the default
trigger timetable instead, the corresponding historical trigger dates are October
19–21. Backfills have their own concurrency limit; use `--max-active-runs 1`.
Manual triggering and native backfill require healthy scheduler/API services;
`dags test` executes locally through Airflow's in-process Execution API.

Actual Phase 3 validation and limitations are recorded in
[reports/phase3_validation.md](reports/phase3_validation.md). Phase 1 and Phase 2
reports remain historical evidence. The following Phase 4 section adds ML training. Docker Compose remains deferred.

## Phase 4 — reproducible ML training

`src/train.py` loads and validates each selected Gold CSV using the existing
`validate_gold_frame`, rejects repeated order IDs across dates, and splits whole
dates before any learned preprocessing. By default **all 46 supplied dates are
required**; missing files/columns, invalid labels/features, empty selections and
fewer than two dates fail clearly. Training must contain both target classes.
The four tracked Phase 2 examples remain unchanged; regenerate the other dates
locally rather than committing 42 more CSVs.

### Regenerate Gold and train

From the repository root, use Python 3.12 and the Airflow 3.1 environment described
above. Install the pinned requirements (the saved artifact uses scikit-learn 1.7.2
and Joblib 1.5.2):

```sh
source /workspace/.delivery-delay/env.sh  # workspace-specific; otherwise activate your venv
python -m pip install -r requirements-dev.txt
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
python -m scripts.generate_gold
python -m src.train
# Optional explicit subset; each listed partition must exist:
python -m scripts.generate_gold --dates 2026-10-01 2026-10-18
python -m src.train --dates 2026-10-01 2026-10-18
```

Generation invokes the existing extraction, Silver, Gold and validation functions
under the same per-date advisory lock as the data DAG. It never edits raw files.
It produces regenerable Bronze/Silver/Gold partitions and quality reports; these
additional generated outputs are ignored by Git. Existing tracked examples stay
tracked. Training reads only Gold; it never generates or repairs data itself.

### Model design and leakage prevention

The baseline is `LogisticRegression(solver="lbfgs", max_iter=2000,
random_state=42)` with default class weighting and threshold 0.5. Its saved
Scikit-learn `Pipeline` contains a `ColumnTransformer` and fitted classifier:

- Numeric: order_hour, items_count, promised_delivery_days, distance_km,
  package_weight_kg, order_value_egp, warehouse_load_pct, carrier_on_time_rate,
  **is_weekend**, **distance_per_promised_day**. Training-only median imputation
  followed by training-only StandardScaler; entirely missing training columns are
  retained with zero imputation.
- Categorical: customer_region, shipping_method, weather_risk_level. Training-only
  most-frequent imputation and OneHotEncoder with unknown categories ignored.
- Excluded: order_id, warehouse_id (identifier fields), raw order_date, and target
  late_delivery. The raw date selects the holdout; its deterministic weekend flag
  is available at order placement. No post-delivery or target-derived input is used.

Silver/Gold only perform fixed data-quality and arithmetic/calendar rules. No
population statistic is learned there. The Pipeline is fitted on the training
partition only. Evaluation reloads the candidate Joblib model and calls `predict`,
without fitting any component. Tests check training medians/scaler means, category
vocabulary isolation, and prohibit `Pipeline.fit` during evaluation.

### Actual chronological holdout and performance

All 46 dates produce **10,053** validated unique orders. Use the earliest
`floor(0.8 * 46) = 36` dates for training and latest 10 dates for testing. Entire
dates stay together and order IDs cannot overlap. Exact test cutoff: **2026-11-06**.
These are supplied assignment dates, including dates later than the execution day;
this is a retrospective exercise, not a claim that future outcomes were observed live.

| Split | Date range (inclusive) | Rows | On time (0) | Late (1) |
|---|---|---:|---:|---:|
| Train | 2026-10-01–2026-11-05 | 7,680 | 5,999 | 1,681 |
| Test | 2026-11-06–2026-11-15 | 2,373 | 1,675 | 698 |

| Metric | Actual test value |
|---|---:|
| Accuracy | 0.733249 |
| Precision (late=1) | 0.619048 |
| Recall (late=1) | 0.242120 |
| F1 (late=1) | 0.348095 |

Confusion matrix, rows actual and columns predicted, label order `[0, 1]`:
`[[1571, 104], [529, 169]]`. This modest baseline misses 529 late deliveries;
accuracy alone overstates usefulness on imbalanced data (always predicting on time
would score 70.59%). No tuning was performed on the test set.

### ML DAG and artifacts

`dags/ml_training_pipeline.py` uses public `airflow.sdk` TaskFlow decorators:

```text
load_gold_data -> train_model -> evaluate_model -> save_model
```

It is manually scheduled, with two retries, one-minute retry delays,
`all_success` dependencies and `max_active_runs=1`. Load validates and writes the
chronological split to `data/ml/<unique-UUID>/split.joblib`; train fits and writes
`candidate.joblib`; evaluate reloads it and writes `evaluation.json`; save verifies
the evaluated candidate SHA-256 and publishes:

- `models/delivery_delay_model.joblib`: fitted preprocessing plus classifier.
- `metrics/metrics.json`: actual scores, confusion matrix, features/model, random
  seed, row counts, exact dates/ranges, split policy and per-split target counts.

Only small directory paths pass via XCom. Each load attempt gets an isolated UUID
folder. A shared POSIX publication lock serializes final writes even across local
CLI invocations; individual files are atomically replaced. The model hash in metrics
links the files. Evaluation failure blocks publication and preserves any prior
final model. There is no minimum score promotion gate in this baseline.

```sh
airflow dags list --local
airflow dags list-import-errors --local
# Real local execution via the in-process Execution API:
airflow dags test ml_training_pipeline 2026-11-06
# With running scheduler/API services:
airflow dags trigger ml_training_pipeline
# Explicit historical subset (default is all 46):
airflow dags trigger ml_training_pipeline \
  --conf '{"dates":["2026-10-01","2026-10-18","2026-10-19","2026-10-20"]}'
# Predict from a trusted saved model and a validated Gold frame:
python - <<'PY'
import joblib
from src.train import load_gold_partitions, FEATURES
frame = load_gold_partitions(dates=['2026-11-15'])
model = joblib.load('models/delivery_delay_model.joblib')
print(model.predict(frame[FEATURES].head()))
PY
```

### Phase 4 testing and limits

Run `python -m pytest -q` for unit tests, and
`RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q` against the configured development
Airflow metadata DB for real executions. Serialize both DAGs first when using a
fresh DB: `airflow dags reserialize`. Actual results: **85 passed, 14 opt-in skips** in the default suite;
**99 passed** with integration enabled. Both DAGs were discovered with zero import
errors, and the full-data ML CLI run succeeded. Full execution evidence
is recorded in `reports/phase4_validation.md` and `reports/phase4_execution.json`.
Tests cover Gold/feature/label failures, cross-date duplicates, insufficient data,
chronology, preprocessing isolation, serialization, saved predictions,
repeatability, DAG discovery/dependencies and real failure propagation.

Workers need shared local storage with POSIX advisory locks; this is not a
remote-object-store implementation. Direct ETL CLI calls bypass partition locks
and must not overlap Gold reads/generation. A Gold snapshot is validated in memory
before staging; raw data must remain immutable. Final model and metrics replacement
are two separate atomic operations, not a single transaction: a process crash
between them can leave mismatched files, detectable with candidate_sha256.
UUID staging directories are intentionally retained for diagnosis; remove obsolete
ones manually after runs finish. Joblib files must come from a trusted source.
Reproducibility requires compatible pinned Python/package versions; numerical
libraries/platform differences can change floating-point results.
No scheduler-service run, UI screenshots, Graphviz output, Docker Compose,
production deployment or merge to main is claimed for Phase 4.
