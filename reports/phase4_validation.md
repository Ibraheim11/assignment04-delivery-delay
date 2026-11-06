# Phase 4 validation — actual results

Scope: ML training only, based on `feature/phase3-airflow-data-dag` at
`0d3bf5e6818a857e44cd0da72e635a46b471b0e3`. All three assignment PDF pages,
data dictionary, ETL source, data DAG, existing tests and README were read first.
The local initial-upload checkout was restored from GitHub content; the recovered
base tree exactly matched `9192042a716b86506995caef2ef606a016f2dd1a`.

## Environment and commands

Executed November 6, 2026, Python 3.12, Airflow 3.1.0, pandas 2.1.4,
NumPy 1.26.4, scikit-learn 1.7.2, Joblib 1.5.2. Used the existing development
virtual environment and metadata DB under `/workspace/.delivery-delay*`.
Repository DAG folder explicitly configured; no Docker Compose was added.

```sh
source /workspace/.delivery-delay/env.sh
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
python -m scripts.generate_gold
python -m src.train
airflow dags list --local
airflow dags list-import-errors --local
airflow dags test ml_training_pipeline 2026-11-06
python -m pytest -q
RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q
```

The first command-network sandbox denied Git transport and stalled the local
Execution API. Network-enabled commands succeeded without changing proxy policy.
An initial integration attempt preceded DAG serialization and failed with
“dag is not serialized”; CLI `dags test` serialized both DAGs, after which the
full suite succeeded. A new unit test initially expected a published model to
be absent during the next training run; that incorrect assertion was corrected.
Only completed successful validation runs count in results below.

## Data, model and leakage checks

All **46** Gold partitions validated; **10,053** unique orders, with no duplicate
IDs across dates. All **46 raw SHA-256 hashes** match the Phase 1 manifest.
The four tracked examples, existing data DAG, ETL implementation and all original
tests are byte-for-byte unchanged from the base commit. Additional generated CSVs
and staging files are ignored, not committed.

Train: earliest 36 dates, October 1–November 5, **7,680** rows (5,999 class 0,
1,681 class 1). Test: latest 10 dates, November 6–15, **2,373** rows (1,675 class 0,
698 class 1). First test date/cutoff **2026-11-06**. These are supplied exercise
dates, including future-dated batches relative to execution day.

LogisticRegression baseline uses 13 approved order-time features including both
Gold features. Identifier fields and raw date are excluded. Training-only median
numeric imputation and standard scaling, and most-frequent categorical imputation
plus one-hot encoding, live inside the fitted Pipeline. Evaluation reloads the
candidate and predicts without fitting. Saved predictions and metrics were
recomputed from the final model and matched exactly.

Actual holdout metrics: accuracy **0.7332490518331226**, precision
**0.6190476190476191**, recall **0.24212034383954154**, F1
**0.34809474768280124**. Confusion matrix `[0,1]`:
`[[1571,104],[529,169]]`. Default threshold 0.5; no holdout tuning or score gate.

## Tests and real execution

- Default suite: **85 passed, 14 skipped** (opt-in execution tests skipped).
- Full integration suite: **99 passed**, including all existing Phase 1–3 tests.
- Both `data_pipeline` and `ml_training_pipeline` discovered; zero import errors.
- New tests cover required/malformed Gold features, invalid targets, missing
  partitions, cross-date duplicates, insufficient data, whole-date splitting,
  training-only imputation/scaling/category vocabulary, no fitting at evaluation,
  Joblib roundtrip and saved predictions, candidate hash protection, isolated
  staging and repeatable outputs, DAG discovery/dependencies and failures.
- Existing real data-DAG tests execute four historical dates twice, inject an
  exhausted failure in each stage and verify a transient extraction retry.
- New real ML-DAG tests execute successfully on isolated selected Gold data and
  inject an exhausted failure at each of its four tasks. Upstream tasks succeed,
  the injected task fails, descendants become `upstream_failed`, and no final
  model is published in failure cases. These failure tests disable retries on
  their test DAG instance; the production DAG retains two retries.
- Full-data CLI execution completed all four required tasks successfully. The
  attached log is actual CLI output with ANSI escapes removed. A second full-data
  CLI execution checked byte-identical model and metrics on rerun.

Actual generation counts, final metadata task states, package versions, hashes,
verification and test output are in `phase4_execution.json`. Actual CLI output is
in `phase4_cli_execution.log` and `phase4_cli_rerun.log`.

## Limits

Local `DAG.test`/CLI execution used Airflow's real in-process Execution API and
metadata database. No queued scheduler/worker-service run or UI screenshots are
claimed. Graphviz is absent and CLI emitted its rendering warning; FastAPI emitted
HTTP 422 deprecation warnings. Same-logical-date local tests replace retained
metadata runs, so the metadata snapshot is not a complete execution history.

Low minority-class recall limits the baseline's practical usefulness. No model
selection or threshold tuning was added. POSIX shared storage/locks are required;
model and metrics publication are separate atomic writes, with a hash to detect
mismatch after a crash. UUID staging files require manual cleanup after completion.
Raw inputs must stay immutable; direct uncoordinated ETL CLI writes must not overlap
training. Library/platform changes may affect floating-point reproducibility.
Docker Compose and remaining deployment/UI evidence are deferred. No merge into
main was performed.
