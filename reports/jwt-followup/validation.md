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

Hosted follow-up execution is pending; no new authentication/runtime success is
claimed before its observed job result and artifact are inspected. Local Docker
storage remains below the established 20 GiB integration preflight, and browser
workflow dispatch was unavailable, so hosted execution is used.
