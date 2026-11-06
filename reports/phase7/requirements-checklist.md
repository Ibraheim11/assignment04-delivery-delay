# Assignment 04 — final requirement checklist

Source: [original PDF](../../Route_Assignment_04_Airflow_Data_ML_Pipelines.pdf),
**all three pages read**, including component responsibilities, evidence table,
final acceptance check and optional bonus. This table maps each normative item;
the PDF's unnumbered architecture diagram is implemented as the sequential
four-task chain specified by its text. Phase 7 changes preserve the Phase 6 ETL,
DAGs, raw data and ML baseline.

Status: **Verified** means source/artifact inspection or recorded execution supports
it. Historical results identify the actual tested commit; final Phase 7 results are
in [validation.md](validation.md) and [audit.json](audit.json).

| PDF page / section | Exact requirement or acceptance criterion | Status | Evidence |
|---|---|---|---|
| 1 / 01 | Use supplied 46 daily orders CSVs, 2026-10-01–2026-11-15, 15-column schema | Verified | [Raw inventory/hashes](../raw_sha256.json); [final byte audit](audit.json); original ZIP |
| 1 / 01 | Predict binary `late_delivery`; standalone assignment repository; preserve original raw files | Verified | [Dedicated repository](https://github.com/Ibraheim11/assignment04-delivery-delay); [dictionary](../../data_dictionary.csv); final ZIP/manifest comparison |
| 1 / 02 | Inspect at least three dated batches in Pandas; identify types, target, identifiers/dates | Verified | [All 46 dated profiles and field roles](../data_exploration.md), [exploration source](../../scripts/explore_data.py) |
| 1 / 02 | Measure missing/invalid values, duplicate orders and class distribution; record README findings | Verified | [README](../../README.md), [JSON](../exploration_summary.json), [profile CSV](../batch_profile.csv) |
| 1 / 02 | Explain Silver fixes versus learned ML preprocessing | Verified | README ETL/methodology; [ETL](../../src/etl.py), [training Pipeline](../../src/train.py) |
| 1 / 03 | Bronze unchanged copy including bad records and duplicates | Verified | Four [Bronze examples](../../data/bronze); ZIP equality, raw/Bronze hashes in final audit |
| 1 / 03 | Silver deduplicates `order_id`, standardizes categories/types, documents missing/invalid rules | Verified | ETL and README; exact duplicates removed, conflicting duplicates fail; [quality reports](../etl) |
| 1 / 03 | Gold suitable predictors, at least two useful engineered features, binary target | Verified | `is_weekend`, `distance_per_promised_day`; [Gold samples](../../data/gold); 13 selected model features |
| 1 / 03 | Validation clearly fails empty data, missing fields, duplicate IDs, incorrect dates and invalid labels | Verified | `validate_base`/`validate_gold_frame`, [ETL regression cases](../../tests/test_etl.py); Silver removes exact duplicates before rejecting conflicts |
| 1 / 04 | DAG named `data_pipeline` with four required dependent tasks | Verified | [DAG](../../dags/data_pipeline.py); exact upstream/downstream assertions in [tests](../../tests/test_data_pipeline.py) |
| 1 / 04 | Daily schedule, at least two retries, one source per logical processing date, idempotent dated outputs | Verified | `@daily`, `retries=2`, explicit date resolver; timetable tests, locks and historical scheduler metadata |
| 1 / 05 | Suitable Scikit-learn classifier trained on validated Gold | Verified | Gold-only loader and LogisticRegression Pipeline; saved-artifact evaluation audit |
| 1 / 05 | Appropriate train/test split; learned preprocessing fitted only on training | Verified | Earliest 36 dates train/latest 10 test; [leakage and split tests](../../tests/test_train.py) |
| 1 / 05 | Report Accuracy / Precision / Recall / F1 and save fitted model with Joblib | Verified | [metrics](../../metrics/metrics.json), [model](../../models/delivery_delay_model.joblib), final recomputation matches |
| 1 / 05 | Second DAG `ml_training_pipeline`, four required tasks; manual trigger after validated Gold acceptable | Verified | [ML DAG](../../dags/ml_training_pipeline.py), [dependencies/execution tests](../../tests/test_ml_training_pipeline.py); manual `schedule=None` |
| 2 / structure | Use student starter baseline; complete Python/DAG functions; outputs/evidence in same repository | Verified with provenance limit | Dedicated initial upload and required layout retained; implemented functions/DAGs, outputs/evidence committed. No separate starter package exists in the supplied repository; personal authorship cannot be established by an artifact audit |
| 2 / 06 | Required DAG/source/raw/layer/models/metrics/screenshots paths | Verified | Repository tree contains every required path; `data/ml` is optional and present |
| 2 / 06 | Dockerfile, compose, requirements, `.env.example`, dictionary and README | Verified | Root deployment/configuration files; pinned dependencies, blank secret placeholders |
| 2 / 07 | `data/raw` preserves all 46 sources and selects by batch date | Verified | Exact inventory plus archive bytes; strict dated paths; no glob/latest fallback |
| 2 / 07 | Reusable dated ETL; data DAG declares/schedules four dependent stages | Verified | `src/etl.py`, `dags/data_pipeline.py`, exact graph/default assertions |
| 2 / 07 | Training loads validated Gold, splits, preprocesses, trains, evaluates and saves; ML DAG orchestrates four stages | Verified | `src/train.py`, ML DAG, successful scheduler execution and failure propagation tests |
| 2 / 07 | Stable per-date layer outputs with no duplicate/appended rows | Verified | Two scratch regenerations for all 46 dates; historical scheduler rerun hashes/row counts; Silver/Gold IDs unique |
| 2 / 07 | Evaluated Joblib model and separate metrics JSON | Verified | Required final paths; model SHA in metrics; saved predictions recomputed without fitting |
| 2 / 07 | Compose gives Airflow access to code, inputs, outputs, logs and artifacts | Verified | Shared/read-only mounts, [configuration tests](../../tests/test_docker_configuration.py), real healthy hosted services |
| 2 / 07 | Screenshots and README explain reproduction | Verified | Genuine Phase 6 captures plus [final ML/rerun screenshots](ui-screenshots.json), successful hosted run and README reproduction commands |
| 3 / 08 | Daily data DAG uses run processing date, two retries, one date per run | Verified | Manual conf dates versus automated intervals tested; production five-minute retries; scheduled-date boundaries documented |
| 3 / 08 | Complete ML chain; manual after valid Gold accepted | Verified | Successful four-task ML scheduler metadata and isolated real Airflow regressions |
| 3 / 08 | Required model/metrics filenames, split strategy and record counts | Verified | 7,680 train / 2,373 test, date ranges, label counts and chronological policy in metrics |
| 3 / 09 | Safe temporary failure followed by automatic retry, `retries=2` | Verified | [Retry metadata](../phase6/retry.json), failed try-1 and successful try-2 task logs/UI; `up_for_retry` timeline; opt-in pre-write fault hook |
| 3 / 09 | Rerun same batch twice; unchanged row counts and no duplicate IDs | Verified | [Idempotency](../phase6/idempotency.json), same-date reprocessing backfill record and higher attempt history; all-layer hashes identical |
| 3 / 09 | Three missed daily batches/backfill and corresponding Gold partitions | Verified | [Historical backfill](../phase6/historical-backfill.json), three successful runs Oct 18–20; three tracked Gold files |
| 3 / 09 | Airflow DAG Graph, task states and execution logs for both pipelines | Verified | Real Phase 6 data graph/attempt logs plus [successful final ML task graph](../../screenshots/phase7/ml-successful-task-graph.png), [ML logs](../../screenshots/phase7/ml-evaluation-task-logs.png) and full hosted task logs |
| 3 / 10 | Launch Airflow with Docker Compose and required path access | Verified | [Phase 5B successful runtime](../phase5b_validation.md), [Phase 6 hosted run](../phase6/workflow-run.json); init/health/build/driver checks |
| 3 / 10 | Complete code/config committed | Verified | Both DAGs/modules, Dockerfile/compose, pinned requirements; default and opt-in regression results |
| 3 / 10 | Generated Bronze/Silver/Gold CSVs for at least three distinct dates | Verified | Four dates per layer: Oct 1, 18, 19, 20; stable counts/hashes in final audit |
| 3 / 10 | Joblib artifact, JSON evaluation metrics and short feature explanation | Verified | Saved model, metrics and README methodology/features/limitations |
| 3 / 10 | Screenshots: successful graphs, logs, retry, same-date rerun, three-date backfill | Verified | Seven authentic Phase 6 captures retained; three [supplementary captures](ui-screenshots.json) verified against the final hosted ZIP, including [rerun tries 1 and 2](../../screenshots/phase7/same-date-rerun-task-history.png) |
| 3 / 11 | New assignment repository, not previous ML project | Verified | Dedicated repository and initial upload history; original assignment ZIP/PDF included |
| 3 / 11 | README scenario, dataset, quality rules, DAGs, model choices, setup and results | Verified | Rewritten final README replaces stale earlier-phase future-tense claims |
| 3 / 11 | Push complete code/sample outputs; no secrets, virtual environments or unnecessary generated files | Verified in final branch | [File/pattern audit](audit.json), ignored runtime files, read-only Actions permissions; final delivery records the pushed commit and PR |
| 3 / acceptance | Reviewer can start Airflow, run dated data DAG, trigger ML, open artifacts and verify reliability evidence | Verified | Reproduction commands plus real hosted runs, model/metric reload, retry/rerun/backfill proofs; final report records actual results and limits |
| 3 / optional | PostgreSQL for processed data | Not implemented; optional | PostgreSQL is Airflow metadata only. Processed data remains CSV |
| 3 / optional | Automatically trigger ML after Gold | Not implemented; optional | Manual ML is explicitly accepted |
| 3 / optional | ETL tests or model versioning | ETL tests implemented; model versioning not implemented | Regression suite; candidate hash integrity is not a model registry/versioning claim |

Additional Phase 7 checks cover link targets, credential-pattern/manual hygiene
review, main/PR regression triggers, manual expensive jobs and a PR targeting
`main` without merge. See the final audit report for actual outcomes and limits.
