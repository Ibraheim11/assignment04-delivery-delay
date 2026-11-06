# Airflow JWT issuer follow-up

Recovery base: merged main `fe73d72a8eaf4e03b2e00d27c5445c84f1d74289`.
PR #1 was already merged by the user; it is not reopened or merged again.

Recovered branch/HEAD: `feature/phase7-final-submission` at `0581d1d0d3baec221a3eb939251614a841fc33c7`.
Four modified files, an untracked compatibility probe, and the completed local
109-pass / 7-warning / 86.70-second test log survived. A binary diff and probe
backup were saved in ignored `work/phase7`; recovered report edits were also saved
in `work/jwt-followup`. No reset, discard or overwrite of pending work was used.
The changes were carried onto `fix/final-airflow-jwt-issuer` based on current main.
Historical Phase 7 reports were restored from their committed content only after
saving the recovered edits; new evidence lives in this separate follow-up folder.

## Root cause and fix

The [actual previous failure](previous-failure.json), downloaded from Actions run
42237540824 with archive hash verification, traces FAB login → Airflow JWT generation
→ PyJWT encode → `TypeError: Issuer (iss) must be a string`.
The Docker build resolved PyJWT 2.15.1, replacing the base image's 2.10.1. The CI
reliability override already supplied a nonempty issuer, but base Compose did not.

The shared Airflow Compose environment now explicitly sets
`AIRFLOW__API_AUTH__JWT_ISSUER=assignment04-phase6-ci` for all Airflow services,
including initialization and CLI. This nonsecret string matches the already
successful hosted authentication configuration. Authentication and signature
validation remain enabled. No dependency downgrade or ML change is introduced.
[PyJWT 2.15.1 probe](compatibility.json) reproduces null rejection, verifies the
shared issuer across all services and successfully encodes/validates a token using
an ephemeral key; token/key contents are never saved.

## Validation

- Docker Compose config validation: exit 0 using an ignored private test env.
- Artifact audit: all 46 raw files match the ZIP/manifest, all 46 dates regenerate
  twice identically, existing model predictions reproduce metrics, ten screenshot
  hashes and local Markdown targets validate. Scratch outputs stay under `work/`.
- Both DAGs, ETL/training source, raw and layer samples, model, metrics, screenshots,
  Phase 6 backfill/retry evidence and historical Phase 7 evidence are unchanged
  from current main (verified with Git diff).
- The surviving pre-interruption regression completed: 109 passed, 7 existing
  FastAPI warnings, 86.70 seconds. Fresh post-recovery regression also passed: **109 passed, 7 warnings, 83.12 seconds**; see [test log](regression.log).
- The existing Docker workflow gains a live HTTP check of FAB token login,
  authenticated DAG API access (both required DAGs) and the UI HTML route. It does
  not claim browser rendering or create/replace screenshots. Tokens and login
  responses are never persisted. A temporary PR trigger launches the verification;
  final configuration returns the expensive workflow to manual-only.

## Observed hosted results

[Docker integration run 42269411399](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42269411399)
completed successfully on `bbc5847fadb1aa1d0243a80fb3a283eec96a5796`, using base
Compose (no reliability-only issuer override). Both DAGs executed through the real
scheduler; saved predictions/metrics/raw hashes matched baseline checks. The live
FAB token endpoint, authenticated DAG API and UI HTML route all passed:
[original observed authentication result](authentication.json). The container suite
passed **109 tests / 7 existing warnings in 77.38 seconds**:
[actual container log](container-tests.log).

[PR regression run 42269411012](https://github.com/Ibraheim11/assignment04-delivery-delay/actions/runs/42269411012)
also passed: **109 tests / 7 warnings in 76.90 seconds**, plus the independent
artifact audit. [Run/artifact metadata](hosted.json) records the tested source SHA.
The downloaded Docker evidence ZIP passed CRC and SHA-256 verification:
`a41690a109815a6f1c51a133de10c46e67fbf32d56e465b87ad5cbe984b7395c`.
The authentication JSON and test log above were copied from it, not reconstructed.

The temporary PR trigger was removed after success; the final Docker workflow is
manual-only and retains the authentication check. Final subsequent edits only
record results, correct README branch/status instructions and remove that trigger;
runtime configuration and authentication check match the successfully tested source.
No new browser rendering/screenshot capture or repeat native backfill is claimed.
Existing screenshots and backfill/retry evidence remain unchanged.

Local Docker storage is below the 20 GiB preflight, so Docker success is from the
actual hosted runner. Browser workflow dispatch was unavailable; a scoped temporary
PR trigger supplied the hosted check instead. Actions downloads expire after 14 days;
selected evidence is committed. Assignment deliverables are preserved and the fix is
verified on this branch; PR #2 remains open for review, with no automatic merge.
