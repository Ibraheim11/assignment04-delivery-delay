# Phase 3 validation — actual results

Base: `feature/phase2-medallion-etl`, commit
`54af59bada565611540f22bf6fb6d70299c8afd6`. All three assignment PDF pages,
`src/etl.py`, existing tests, README and Phase 1/2 reports were reviewed first.
Scope is the data DAG only. No ML or Docker Compose implementation.

## Environment

Python 3.12, Apache Airflow **3.1.0**, existing environment at
`/workspace/.delivery-delay-venv`, development metadata DB at
`/workspace/.delivery-delay/airflow`. The DAG folder was explicitly set to this
repository's `dags/`. Tests ran November 6, 2026. Machine timestamps in attached
execution records are UTC.

## Results

- Original baseline before changes: **23 passed**.
- Full suite, including opt-in real Airflow executions:
  `RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q`:
  **62 passed, 7 warnings in 58.40s**. This is 23 unchanged ETL tests,
  30 DAG unit/configuration tests and 9 real execution tests.
- Alternate daily interval configuration:
  `AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=True python -m pytest tests/test_data_pipeline.py -q`:
  **30 passed in 2.38s**. The normal environment uses Airflow's default
  `create_cron_data_intervals=False` trigger timetable.
- CLI discovery: `airflow dags list --local -o json` returned `data_pipeline`
  from `dags/data_pipeline.py`, paused. `airflow dags list-import-errors --local -o json`
  returned `[]`. DagBag import and exact graph assertions also passed.
- Tests check the four-task chain, `all_success`, daily UTC schedule,
  `catchup=False`, concurrency limit, two retries and five-minute delay.
- Date tests cover scheduled/backfill interval and trigger timetables, manual
  overrides with unrelated intervals, absent/manual invalid dates, forbidden
  automated overrides and invalid interval boundaries. Missing files, including
  November 16, raise rather than selecting another source. Lock contention and
  lock release after failure are tested.

## Actual Airflow executions

A real CLI invocation succeeded:

```sh
airflow dags test data_pipeline 2026-10-01 -c '{"batch_date":"2026-10-01"}'
```

All four task states were `success`; Gold validation returned 180 rows and zero
duplicate IDs. The attached CLI log is actual output with ANSI color escapes
removed; it is not reconstructed.

The opt-in integration tests use `DAG.test()` and Airflow's real in-process
Execution API, configured metadata DB, real TaskFlow/XCom and task state handling.
Data writes are isolated in temporary directories. Each of the following batches
ran twice successfully, with only its own Bronze/Silver/Gold partition created,
unchanged bytes on rerun, correct Gold order dates, and these validated counts:

| Batch | Bronze rows | Silver/Gold rows | Successful test runs |
|---|---:|---:|---:|
| 2026-10-01 | 183 | 180 | 2 |
| 2026-10-18 | 248 | 245 | 2 |
| 2026-10-19 | 183 | 180 | 2 |
| 2026-10-20 | 196 | 193 | 2 |

Four additional real failure cases intentionally fail each task in turn.
Preceding tasks succeed, the injected task fails, and every dependent task is
`upstream_failed`. These tests set retries to zero on a separate test DAG instance
to verify exhausted-failure behavior without waiting ten minutes. Production
retry settings remain unchanged and are asserted separately.

A real transient-failure case makes extraction fail once, then succeed on retry;
all downstream tasks succeed. Its test DAG retains two retries but uses a zero
retry delay. The metadata export confirms extraction `try_number=2`.
`reports/phase3_execution.json` is an actual metadata query after testing.
Airflow's local `DAG.test` replaces same-logical-date records, so that export
contains only the latest retained runs, not all 13 test executions. Successful
pytest assertions establish the earlier reruns and failure states.

## Limits and preserved work

These were local Airflow test executions, not scheduler-driven backfills or
worker-service executions. Native backfill and queued manual-trigger commands
are documented but were not executed. No UI screenshot or Graphviz rendering is
claimed; Graphviz is absent. Airflow/FastAPI emitted deprecation warnings.

The first CLI attempt stalled inside the Execution API under the restricted
command network sandbox and was terminated. Repeating with command network access
succeeded; the integration suite used the same command network access. Git fetch
also required command network access. This did not require changing proxy policy
or the DAG. The abandoned attempt is not counted as a successful execution.

All Phase 1/2 source, tests, raw files, generated sample partitions and existing
reports are unchanged. Per-date writes require shared POSIX storage with working
advisory locks. Direct ETL CLI execution bypasses the DAG lock and must not overlap
DAG writes. Deployment, Docker Compose, scheduler/UI evidence and ML remain later
work. No merge into main is performed.
