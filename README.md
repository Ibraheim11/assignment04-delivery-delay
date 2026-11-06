# Delivery delay prediction — Assignment 04

Phase 1 is complete: the supplied daily delivery orders are extracted unchanged,
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
  weather risk 94 and carrier reliability 87 (646 missing cells in five fields).
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

## Later-phase design decisions

Silver should validate dates and labels, deduplicate IDs deterministically, normalize
categories and types, and convert invalid feature values to missing with quality counts.
Conflicting duplicate IDs need an explicit resolution policy. Learned imputation,
encoding, scaling and feature selection belong in a training-only fitted pipeline
after a chronological split. Exclude order IDs from features. Candidate Gold features
include weekday/weekend and distance per promised delivery day. These are proposals;
none is implemented in Phase 1.

## Structure and scope

```text
data/raw/                     46 original CSVs, unchanged from ZIP
scripts/explore_data.py        extraction, verification and Pandas exploration
reports/                      reproducible findings and integrity manifest
dags/, src/                   placeholders for later implementation
data/bronze/, silver/, gold/  placeholders for later dated outputs
data/ml/, models/, metrics/   placeholders for later intermediates/artifacts
screenshots/                  placeholder for later Airflow evidence
requirements.txt              Phase 1 dependencies
.env.example                  configuration placeholder
```

No student starter code was included in the repository or ZIP. The original archive,
PDF and dictionary remain unchanged. ETL modules, DAG files, Dockerfile/Compose,
training, model artifacts, evaluation metrics and Airflow evidence are deferred.
No Airflow or Docker execution was performed. Stop here for the Phase 1 review.
