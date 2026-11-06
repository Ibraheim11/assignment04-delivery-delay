# Phase 5B — actual GitHub-hosted Docker runtime verification

**Passed.** The complete runtime job succeeded on November 6, 2026:

- [Successful workflow run](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42227343176)
- Verified code commit: `83dcc4e2fb782a83503396bccb7a8f8054d5a3e5`
- [Actual evidence artifact](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42227343176/artifacts/13506538230)
- Artifact: `docker-runtime-42227343176-1`, SHA-256
  `761ff2526ef5c6f8779dbbb3581442916062c3b6c8ed78a07f4db7a47c19c00d`.

Base: `feature/phase5-docker-airflow` at
`e18cbfff9db3a34227b9584d2b25b03032b87609`. Only this verification workflow,
its helpers/tests, the required image driver dependency and documentation changed.
Both DAGs, ETL/training implementation, raw CSVs, previous tests and checked-in
model/metrics are preserved. The workflow does not push runner outputs to Git.
The follow-up results commit is documentation only and uses `[skip ci]` to avoid
repeating the already successful runtime test for documentation changes.

## Actual runner and build

The push-triggered workflow used a fresh GitHub-hosted `ubuntu-24.04` runner.
Docker version/driver, available memory, filesystem free space and Docker disk
usage are recorded in the artifact's resource logs. The 20 GiB disk / 4 GiB RAM
pre-build guard passed. No safe-SDK cleanup was needed on the successful runner.
No changes were made to other machines, projects or Docker storage drivers.

`docker compose config --quiet`, `docker compose build`, PostgreSQL startup,
Airflow initialization and required-service startup all returned **exit code 0**.
The runtime image remained the official slim Airflow 3.1.0 Python 3.12 image.
Installing `asyncpg==0.30.0` was necessary: Airflow 3.1 initializes an asynchronous
PostgreSQL engine as well as the psycopg2 synchronous connection. This version
matches Airflow 3.1.0's official Python 3.12 constraints.

## Services and DAG validation

PostgreSQL, API/UI server, scheduler, DAG processor and triggerer each reached
Docker **healthy** status. Startup has explicit timeouts; the original inspect
observations are in `health-summary.json` and `final-health-*.log` in the artifact.
The existing API healthcheck `/api/v2/version` succeeded and was retained.

In-container discovery verified Airflow **3.1.0**, Pandas **2.1.4**, NumPy **1.26.4**,
Scikit-learn **1.7.2**, Joblib **1.5.2**, both required DAG IDs, zero import errors
and both exact four-task dependency chains. Raw/source/model/report bind mounts
were accessible at the existing shared paths, retaining POSIX locks.

## Real scheduled task execution

These were externally triggered manual DagRuns handled by the running scheduler,
LocalExecutor subprocesses and Execution API, **not `DAG.test()`**:

1. `data_pipeline`: explicit manual conf `{"batch_date":"2026-10-18"}`.
   All four required task instances were observed in `success` state before the
   workflow continued. The supplied Gold partition contains 245 validated rows.
2. The existing generation script ran inside the container and validated all
   **46** supplied Gold partitions.
3. `ml_training_pipeline`: all four task instances were observed in `success`.
   Final verification loaded the published model, made predictions, checked the
   artifact hash, validated all Gold, recomputed held-out metrics and compared
   them with the committed Phase 4 baseline. All checks passed.

Original task-instance observations and task logs are uploaded as
`task-states-data_pipeline.json`, `task-states-ml_training_pipeline.json` and
`task-logs/`; they are not reconstructed task states. A CI-only override disables
automatic date-scheduled runs, avoiding nonexistent future batches while leaving
manual runs and the production daily schedule unchanged. Each tested DAG is
paused again after its run completes.

The dataset remains **10,053 unique orders**: train **7,680**, October 1–November 5;
test **2,373**, November 6–15. Docker metrics matched the recorded baseline within
1e-12: accuracy **0.733249**, precision **0.619048**, recall **0.242120**, F1
**0.348095**; exact confusion matrix/counts/date ranges/features matched.
All **46 raw hashes** matched before and after execution. The runner's model and
metrics are included as evidence; no baseline artifact was replaced in Git.

## Tests and evidence handling

- Complete host suite: **107 passed**, seven existing FastAPI deprecation warnings.
- Complete container regression suite: **passed, exit code 0**, including opt-in
  real Airflow tests in an isolated SQLite development metadata DB. Its exact
  pytest output is `container-tests.log` in the uploaded artifact.
- New helper tests verify secret/JWT redaction, actual failure exit-code recording
  and observed-state decoding. Existing tests were preserved.
- Image build, initialization, service health, discovery, task states, task logs,
  Gold/model checks and test results are actual command output with exit codes.
  See `reports/phase5b_execution.json` for a compact audit summary and job excerpt.

The workflow has only `contents: read`, does not persist checkout credentials,
generates a private ephemeral `.env`, masks its values in Actions output and
redacts them plus JWT/Bearer tokens before writing evidence. Only the evidence
directory is uploaded. Container environments, .env and metadata DBs are excluded.
Collection/upload run with `always()` and preserve failures. Containers and the
disposable database volume are removed afterward. Artifacts expire after 14 days.

## Diagnosed earlier attempts

- [Run 42226204716](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42226204716):
  image build succeeded; initialization failed with missing `asyncpg`. The pinned
  asynchronous driver was added.
- [Run 42226733420](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42226733420):
  all services, both real DAG runs and artifact verification passed. Its separate
  test container failed because overriding the Airflow entrypoint bypassed the
  arbitrary-UID Python environment setup. The test launcher now invokes `bash`
  through the normal entrypoint, and the successful run verified the correction.

No unresolved runtime blockers remain on the GitHub-hosted runner. Codex Cloud's
small vfs Docker disk remains unsuitable for local builds; it was not used to
claim runtime success. This is transient integration verification, not production
hardening, Windows Docker Desktop setup, or final backfill/UI evidence collection.
Nothing was merged into main and no screenshots were fabricated.
