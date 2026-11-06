# Phase 5 — Docker deployment validation

Base: `feature/phase4-ml-training-dag`, commit
`b467122f06f56e10ad4f72ad2047ba3e48037e28`. All three assignment PDF pages,
README, both DAGs, ETL/training modules, existing tests and the fitted Joblib model
were reviewed. Scope is Docker deployment/integration configuration only; no final
backfill or UI evidence collection, no model redesign and no merge to main.

## Docker availability and actual build attempts

Docker CLI **29.9.2**, Compose **2.40.3**, and the managed Linux Docker daemon
**29.9.2** are available when the command has network/socket access. The daemon is
rootless with the **vfs** storage driver. It reports 3 CPUs and about 9.7 GiB RAM,
but its filesystem is only about **8 GB**. The default restricted command sandbox
initially denied socket access; the network-enabled local-socket diagnostic
succeeded. No remote Docker endpoint or host-network bypass was used.

Actual attempts on November 6, 2026:

1. `docker compose build --build-arg BUILDKIT_INLINE_CACHE=1` using the full official
   Airflow 3.1.0 Python 3.12 image failed with `ResourceExhausted` / `no space left
   on device` during image extraction. A concurrent base-image pre-pull was
   cancelled and this attempt's build cache cleared.
2. Changed to the smaller official `apache/airflow:slim-3.1.0-python3.12` image,
   explicitly adding the FAB provider and PostgreSQL driver in the Dockerfile.
   `docker build --target runtime --secret
   id=proxy_ca,src=/etc/ssl/certs/ca-certificates.crt
   -t assignment04-airflow:3.1.0 .` independently failed while registering a base
   layer, again with `no space left on device`. It never reached project pip
   installation. TLS verification remained enabled; the session CA was mounted as
   a BuildKit secret rather than copied into the image.
3. Cleared this task's failed build cache. No project image or containers remain.
   No daemon storage-driver changes or deletion of another project's images or
   volumes were performed.

**Image build did not succeed.** Initialization, Compose service startup,
container health checks, in-container DAG discovery and container DAG executions
were **not run**, because the required image could not be built. There are no
Docker-generated metrics to compare with Phase 4. Raw build logs are preserved
in `phase5_execution.json`; static and host checks below do not establish Docker
runtime success. Re-run the documented commands on a Docker host with sufficient
storage to finish container validation.

## Implemented configuration

The Compose layout adapts the official Airflow 3.1.0 quickstart from the tagged
Apache GitHub source. The documentation website was denied by the environment's
network policy, so the official tagged GitHub source was used. LocalExecutor
replaces Celery for this small assignment; scheduler subprocesses execute tasks.
PostgreSQL 16, initialization, API/UI server, scheduler, separate DAG processor,
triggerer and an optional CLI service are configured. No broker is required.

Every Airflow component shares the same `/opt/airflow` project paths and
PYTHONPATH, retaining existing POSIX locks. Code and raw inputs are read-only;
outputs and logs are persistent writable host binds. PostgreSQL uses a named
volume. UI binding is limited to `127.0.0.1:8080`; DB is not published. Health
checks and successful-init gates protect startup order. New DAGs default paused,
while both existing DAG IDs, daily/manual schedules and dependencies are preserved.

The image installs as the Airflow user, explicitly preserves Airflow 3.1.0,
installs the Phase 4 model-library pins and checks dependencies during a future
successful build. No training occurs in the Dockerfile. An optional test target
installs development dependencies. `.dockerignore` allows only build dependency
files, excluding secrets, source data and artifacts from the build context.
The setup script creates a private ignored `.env` with random local secrets and
output directories, refuses to overwrite it, and prints no credentials.

## Actual static and host verification

- `docker compose config --quiet`: **passed** against a locally generated private
  `.env`; resolved configuration was not printed or committed.
- `docker build --check .`: **passed, no warnings**. This is a Dockerfile static
  check, not an image build or runtime check.
- Default host suite: **90 passed, 14 skipped** (execution tests are opt-in).
- Complete host suite including real Airflow integration tests: **104 passed**.
  This includes all **99** prior tests and five new deployment tests. New checks
  cover supported service dependencies and health gates, identical shared paths,
  read-only raw mount, localhost exposure, required secrets, paused defaults,
  Airflow/model pins, and setup secret permissions/refusal to overwrite.
- Both DAGs discovered on the **host**, with zero import errors and the expected
  four-task dependency chains. Existing integration tests exercised real local
  Airflow DagRuns, including the existing data/ML failure and retry tests.
- The smoke script was executed on the **host** as a preflight test: all five
  package versions matched, all 46 raw hashes matched, all 46 Gold partitions
  validated (10,053 rows), the committed model loaded and predicted 245 sample
  rows, and its held-out metrics exactly reproduced Phase 4. This did not train
  a new model and did not execute inside the blocked image.
- Both DAG modules, ETL/training logic, existing tests, all raw CSVs, four committed
  layer examples, saved model and metrics remain unchanged from Phase 4.

The setup test initially caught use of an unsupported `Path.open(opener=...)`
argument; this was corrected to built-in `open` and the full suite rerun. Actual
successful test output, host preflight and build/static diagnostics are retained
in `phase5_execution.json`. No screenshots or successful container logs were created.

## Remaining environment limits

Container runtime integration remains unverified because of image-layer storage
exhaustion. Provision adequate Docker storage (at least 20 GB free; more for vfs)
and follow the README to build, initialize, start, inspect health, run the dated
October 18 data DAG, regenerate all 46 Gold partitions and execute the ML DAG.
Then run the container smoke script with the Phase 4 baseline comparison.

UID mapping and shared advisory locks must work on the deployment host; rootless
engines may require bind-mount permission adjustments. The local-only quickstart
uses development credentials from .env and is not production hardening. Image tags
and transitive dependency resolution can change; the explicit Airflow and model
library versions are fixed, but a complete transitive image lock is not provided.
Final backfill, scheduler/UI evidence collection and production deployment are
outside this phase. No merge into main was performed.
