# Phase 6 observed reliability results

Execution: https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42237926425

Tested source commit: `e1dbe3f46906337ffe7b715be76a08d35594ff3b`.
Base: `93513d917fa389b1428762376d8346c6c4b2b8bc`.

Environment: GitHub-hosted Ubuntu 24.04, Docker Compose, Airflow 3.1.0, PostgreSQL and LocalExecutor. Automatic schedules were disabled in CI.

| Batch date | Logical date / interval start (UTC) | Interval end (UTC) | Task states | Bronze / Silver / Gold rows |
|---|---|---|---|---|
| 2026-10-18 | 2026-10-18 00:00:00+00:00 | 2026-10-19 00:00:00+00:00 | 4 success | 248 / 245 / 245 |
| 2026-10-19 | 2026-10-19 00:00:00+00:00 | 2026-10-20 00:00:00+00:00 | 4 success | 183 / 180 / 180 |
| 2026-10-20 | 2026-10-20 00:00:00+00:00 | 2026-10-21 00:00:00+00:00 | 4 success | 196 / 193 / 193 |

## Idempotency

October 18 was backfilled twice through the real scheduler. Counts and complete SHA-256 hashes matched byte for byte. Silver and Gold had zero duplicate order IDs. No rows were appended. All 46 original raw hashes and every other output date remained unchanged.

| Output (October 18) | Rows | SHA-256, identical on both executions |
|---|---:|---|
| bronze | 248 | `a9cad37c2bbcab454785717bb3b164d1697cb33e199e3f759354d57ecd23c361` |
| silver | 245 | `9c356701a96dec6695c1b415561023416b2e62c6674c462b86015bd1f19df66d` |
| gold | 245 | `dd505ef61d2145f227a8ce4ea5a5d817d286d98ecbd4790bc6c3bbf89d4c2177` |

## Retry and failure propagation

Retry run: `phase6-retry-42237926425-1`. Extraction try 1 is `failed` in real Airflow attempt history; `up_for_retry` was observed in live metadata. The automatic retry succeeded on try 2. All four tasks finished `success`.

Exhaustion run: `phase6-exhaust-42237926425-1`. Extraction failed after three attempts (retries=2); all three downstream tasks were `upstream_failed`, with zero attempts and no worker PID. Output artifacts were unchanged.

## Tests and live UI evidence

Container regression suite: `109 passed, 7 warnings in 68.88s (0:01:08)`. Both DAGs were discovered without import errors. Local production-default regression: 95 passed, 14 opt-in integration tests skipped. An attempted local opt-in execution was interrupted; the hosted container suite is the complete integration result.

The latest data_pipeline run appears red in the DAG list/overview because it is
the deliberately exhausted failure-propagation run. The backfill and retry screenshots
show their successful task states. The workflow itself completed successfully.

7 real live Airflow UI screenshots:

- [both-dags.png](../../screenshots/phase6/both-dags.png)
- [data-pipeline-graph.png](../../screenshots/phase6/data-pipeline-graph.png)
- [ml-training-graph.png](../../screenshots/phase6/ml-training-graph.png)
- [historical-backfills.png](../../screenshots/phase6/historical-backfills.png)
- [backfill-successful-tasks.png](../../screenshots/phase6/backfill-successful-tasks.png)
- [retry-final-success-logs.png](../../screenshots/phase6/retry-final-success-logs.png)
- [retry-first-failed-attempt-logs.png](../../screenshots/phase6/retry-first-failed-attempt-logs.png)

## Limits and evidence interpretation

- CI retry delay is 30 seconds; production remains five minutes with retries=2.
- Airflow may assign timestamps while marking upstream_failed. Zero attempts and no worker PID establish downstream nonexecution.
- Partitions use the existing date-specific CSV filename layout rather than new date directories.
- The scheduler marks completed parent Backfill records in a periodic sweep; the rerun waits for that real metadata transition.
- Task-log filenames are URL-encoded for Actions artifact portability; contents and metadata run IDs retain the genuine evidence.
- Credentials, JWTs, .env, databases and browser storage are excluded. Logs are sanitized.
- The live UI split panes clip some log text/table columns at this viewport; the full genuine task logs and metadata are committed alongside the screenshots.
- Extended Actions artifacts expire after 14 days; selected reports, test logs, retry logs and screenshots remain in Git.

Command exit codes: [command-exits.json](command-exits.json). Full metadata: [final-metadata.json](final-metadata.json). Full per-date row counts and hashes: [idempotency.json](idempotency.json).

Additional evidence checks after download: all screenshot SHA-256 hashes matched, selected Bronze hashes matched their intended raw CSVs, Silver/Gold duplicate counts were zero for all three dates, and all 46 raw hashes still matched the final snapshot.
