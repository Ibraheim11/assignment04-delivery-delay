# Phase 7 — final audit and submission preparation

Audit date: November 6, 2026. Repository: `Ibraheim11/assignment04-delivery-delay`.
All three pages of the original PDF were read. The [requirement-by-requirement
checklist](requirements-checklist.md) includes all eleven numbered sections,
component responsibilities, the evidence table, final acceptance and optional items.

Base branch `feature/phase6-reliability-evidence` was fetched and its exact head
verified as `3f0c9832a1d29490e3d422a5130f51b9db17d435` before creating
`feature/phase7-final-submission`. The initial local checkout was the old upload;
it was clean and no user edits were overwritten.

## Verified deliverables and preserved implementation

- All 46 raw files have the exact expected dates and match both the original ZIP
  bytes and the recorded SHA-256 manifest. The dictionary also matches the ZIP.
- All four example dates exist in Bronze/Silver/Gold. Bronze matches raw bytes;
  Silver/Gold IDs are unique and Gold validates. Counts are 183/180/180 (Oct 1),
  248/245/245 (Oct 18), 183/180/180 (Oct 19), 196/193/193 (Oct 20).
- The independent [artifact audit](audit.json) regenerates all 46 dates **twice**
  in disposable `work/`, checks identical hashes, and verifies the committed
  examples match. All Gold combines into 10,053 unique orders.
- Both DAGs have the exact four required tasks/dependency chains. Daily/manual
  date resolution, missing sources, retries, locking, failure propagation and
  ML preprocessing isolation are covered by tests and hosted execution evidence.
- The existing saved Pipeline is loaded and evaluated without fitting. Its hash
  and every persisted metric/count/split field match. Accuracy 0.7332490518331226,
  precision 0.6190476190476191, recall 0.24212034383954154, F1 0.34809474768280124;
  confusion matrix `[[1571,104],[529,169]]`. Train/test rows: 7,680 / 2,373.
- `src/etl.py`, `src/train.py`, both DAGs, all original inputs, the saved model,
  metrics, four layer examples and Phase 6 evidence are byte-for-byte unchanged
  from the expected Phase 6 base. No baseline redesign or retraining was committed.

Original source digests: PDF `3caf111678da31883558f7d509d3436898acd3f24dd81c62f24f454a0975029c`,
ZIP `6358d3b79007473fbace8c15795600ac1a2c58c36a3ce95c131ef3234b1fab3b`.
Saved model digest: `b7a8a2d004be6b8a8310803343d321246d6c2f346dfb280f04c5eec940e82cd5`.

## Problems found and necessary fixes

1. The README was a cumulative phase diary with stale claims that ML/preprocessing
   remained future work and that no artifacts existed. It is now a final overview
   with architecture, exact quality/feature rules, setup/Docker/host reproduction,
   actual metrics/limitations, dates/reliability, evidence and requirement mapping.
2. Both expensive workflows targeted obsolete feature branches. Final configuration
   makes them manual-only. A lightweight regression workflow runs on PRs targeting
   `main`, pushes to `main` and manual dispatch. It migrates/serializes an isolated
   SQLite development database and runs the full Airflow suite plus artifact audit.
3. The Phase 6 workflow had write permission and a hardcoded push to its old branch.
   It now uses read-only contents permission and artifact upload; it never commits
   runner outputs or changes any branch. A one-time restricted Phase 7 push starts
   final evidence before new workflows are available for default-branch dispatch;
   that bootstrap trigger is removed from the submitted workflow.
4. Seven real screenshots existed, but ML task-state/log coverage was incomplete
   and rerun proof was mostly metadata. Capture tooling now requests a successful
   per-run ML graph, real evaluation logs and a same-date rerun task-history view.
   Missing required screenshots or capture limitations fail the workflow. Archive
   tooling also preserves sanitized ML task logs and observed states.
5. Fresh-host testing revealed that database migration/serialization is needed even
   for a default DAG-discovery test. README/CI now explicitly perform both steps.
6. The Phase 5B direct artifact page returned 404 to signed-out reviewers. Its
   report now links to the public workflow summary, retains the original artifact
   ID/hash and explains that downloading Actions artifacts requires GitHub sign-in.

## Actual local regression and setup results

Python 3.12.14, Airflow 3.1.0, Pandas 2.1.4, NumPy 1.26.4, Scikit-learn 1.7.2,
Joblib 1.5.2, pytest 8.3.5. The existing virtual environment was used outside the
repository; the final development metadata database is under ignored `work/phase7`.

| Check | Observed result | Durable evidence |
|---|---|---|
| Dependency consistency | `No broken requirements found` | [Dependency check](dependency-check.log) |
| Default regression after migration/serialization | **95 passed, 14 skipped**, 7.56 seconds | [Unit test log](unit-tests.log) |
| Opt-in real Airflow regression with local socket access | **109 passed, 7 warnings**, 84.78 seconds | [Complete integration log](integration-tests.log) |
| All raw bytes, all-date reruns, saved predictions/metrics, screenshots, hygiene and local Markdown targets | Passed | [Audit JSON](audit.json) |
| Compose interpolation/validation | `docker compose --env-file work/phase7/compose-validation/.env -f docker-compose.yml config --quiet`, exit 0 | Private generated test env stayed in ignored `work/`; no secret values captured |
| Changed Python syntax and workflow YAML parsing | Passed | AST parse of changed scripts; BaseLoader trigger inspection |
| Git whitespace check | `git diff --check`, passed | Final pre-commit verification |
| Reachable text-history credential-pattern scan | 246 objects checked, no high-confidence findings | [History scope/results](history-hygiene.json) |
| Public external documentation links | Eight distinct HTTPS links return 200 after correcting the signed-out artifact link | [External link observations](external-links.json) |
| Existing Phase 6 screenshot provenance | All seven committed PNGs match the original successful Actions ZIP bytes | [Artifact verification](phase6-artifact-verification.json) |

The 14 default skips are deliberate opt-in executions; all run in the 109-test
suite. Seven warnings are existing FastAPI `HTTP_422_UNPROCESSABLE_ENTITY`
deprecations. Intentional failed task cases pass only when downstream states and
artifact nonpublication match expectations.

Initial attempts are preserved as limitations: the unconfigured command hit the
read-only home directory and produced three collection errors; a scratch Airflow
home without migration produced 94 passes, 14 skips and one missing-table failure.
A migrated attempt without local-socket access was interrupted after 30 passes
(37.26 seconds); it is not counted as a complete result. The properly configured,
network-enabled local execution above completed all 109 tests. The audit scanner
initially recognized the deliberately fake short JWT fixture as a credential;
its high-confidence JWT pattern was corrected to require realistic segment lengths.
These were setup/audit-tool issues; production ETL, DAG and ML code was not changed.

## Hosted Docker/runtime and supplementary UI evidence

Existing successful evidence remains unchanged:
[Phase 5B run 42227343176](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42227343176),
[Phase 6 run 42237926425](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42237926425),
and their [Phase 5B](../phase5b_validation.md) / [Phase 6](../phase6/validation.md) reports.
The latter's selected committed screenshot bytes were independently checked against
its original artifact with SHA-256
`5cd1a9dad604a89cb73e9c936fce2e838d1cb1a45b9d1ad08d41349985db719c`.

Final supplementary execution is [run 42242189893](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42242189893)
on source commit `ab471c3ea0f145d2b985e207ba86e4199ab43bd1`.
**Completed successfully.** The workflow built the image, started healthy PostgreSQL/
Airflow services, executed both DAGs through the real scheduler/LocalExecutor,
recomputed baseline metrics, ran **109 container tests (7 existing warnings,
59.38 seconds)**, completed the three-date native backfill, reprocessed October 18
with identical hashes/counts, observed retry success on try 2, and verified that
an exhausted try-3 failure blocked all descendants without changing outputs.

All ten live UI captures succeeded with no reported capture failures. The seven
original Phase 6 images remain unchanged; three supplementary images are committed:

- [Successful ML task graph](../../screenshots/phase7/ml-successful-task-graph.png).
- [Live ML evaluation task logs](../../screenshots/phase7/ml-evaluation-task-logs.png).
- [Same-date rerun, successful tries 1 and 2](../../screenshots/phase7/same-date-rerun-task-history.png).

The rerun screenshot's Airflow breadcrumb displays October 19 (the run-after /
interval end); metadata and processing-date logs establish the October 18 batch.
Complete values are available in the original task logs where UI panes clip lines.

[Workflow/jobs/artifact metadata](hosted-workflow.json), [screenshot hashes and
original routes](ui-screenshots.json), [container test output](hosted/regression-tests.log),
[all four successful ML task states](hosted/task-states-ml_training_pipeline.json),
[full sanitized task logs](hosted/task-logs), [backfill](hosted/historical-backfill.json),
[idempotency](hosted/idempotency.json), [retry](hosted/retry.json),
[failure propagation](hosted/failure-propagation.json), and
[final artifact verification](hosted/final-artifact-verification.log) are durable
selected evidence copied from the actual ZIP. The original ZIP passed CRC and
SHA-256 verification: `ebecec885e6ffd7140453670927b814b1bbcfa887739c13c3a6768dbc28b1b52`.
PNG bytes and sanitized logs were copied without reconstructing their contents.
The ZIP itself stays outside Git because the selected evidence is sufficient.

## Hygiene, evidence interpretation and remaining limits

No virtual environment, database, `.env`, browser storage, cache, temporary work
or unnecessary generated partitions is committed. Source/category/logging rules
and workflow permissions were manually reviewed alongside automated pattern checks.
Patterns cannot establish absence of every possible secret format; the scanner's
scope is recorded. Original archive/PDF/model artifacts are intentional deliverables.
Local Markdown link targets are checked across the repository; source-run URLs are
verified through GitHub. Historical phase reports retain their original scope and
are not presented as final-state claims.

Low minority recall, finite exercise dates, shared POSIX storage, individual-file
atomicity, two-file model/metrics publication, retained UUID staging and package
compatibility limitations remain documented. No processed-data PostgreSQL store,
automatic ML trigger or model registry is claimed. A separate supplied starter
package/personal-authorship provenance cannot be independently established from
this repository; the required layout and original assignment upload are preserved.

This local managed runtime has only about 7.5 GiB free with Docker's `vfs` driver,
below the integration runner's 20 GiB preflight. Docker runtime claims therefore
come from actual GitHub-hosted execution, not an attempted local image build.
Extended Actions uploads expire after 14 days; selected evidence remains in Git.
UI panes can clip long lines; complete sanitized task logs complement captures.
Top-level library versions are pinned; image tags/transitive packages are not a
complete reproducible image lock, and future builds may resolve them differently.

No unresolved pipeline/runtime/UI-capture blocker remains. Submission branch:
`feature/phase7-final-submission`; the final delivery reports its pushed commit SHA
and PR URL targeting `main`. The PR is left open; no merge is authorized or performed.
