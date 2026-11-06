"""Phase 6: real scheduler/backfill/retry evidence on a disposable hosted runner."""
import json
import os
from pathlib import Path
import shutil
import time
from scripts import docker_integration as runtime

ROOT = runtime.ROOT
OUT = ROOT / 'reports/phase6'
DATES = ['2026-10-18', '2026-10-19', '2026-10-20']
TASKS = ['extract_to_bronze', 'transform_to_silver', 'prepare_gold', 'validate_gold']
TIMELINE = []


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(runtime.redact(json.dumps(value, indent=2, sort_keys=True)))


def probe():
    _, output = runtime.container('phase6-probe', ['python', '-m', 'scripts.reliability_probe'])
    return runtime.decode_probe(output)


def wait(select, expected=1, failed=False, timeout=900):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        observed = probe()
        runs = [r for r in observed['runs'] if select(r)]
        entry = {'observed_at_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'runs': runs}
        if not TIMELINE or TIMELINE[-1]['runs'] != runs:
            TIMELINE.append(entry)
            save('metadata-timeline.json', TIMELINE)
        if len(runs) == expected and all(r['state'] == ('failed' if failed else 'success') for r in runs):
            if not failed:
                for run in runs:
                    assert {t['task_id'] for t in run['tasks']} == set(TASKS)
                    assert all(t['state'] == 'success' for t in run['tasks'])
            return observed, runs
        if not failed and any(r['state'] == 'failed' for r in runs):
            raise AssertionError(f'Unexpected failed run: {runs}')
        time.sleep(5)
    raise TimeoutError('Phase 6 scheduler execution exceeded bounded wait')


def backfill(start, end, name):
    args = ['airflow', 'backfill', 'create', '--dag-id', 'data_pipeline',
            '--from-date', start, '--to-date', end, '--max-active-runs', '1',
            '--reprocess-behavior', 'completed']
    runtime.container(name + '-dry-run', args + ['--dry-run'])
    runtime.container(name, args)


def trigger(run_id, fault):
    runtime.container('trigger-' + fault, ['airflow', 'dags', 'trigger', 'data_pipeline',
                      '--run-id', run_id, '--conf', json.dumps({'batch_date': DATES[0], 'phase6_fault': fault})])


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('RUNNER_ENVIRONMENT') != 'github-hosted':
        raise RuntimeError('This destructive-to-disposable-runner integration requires GitHub-hosted Actions')
    os.environ['ASSIGNMENT04_RELIABILITY_CI'] = 'true'
    # Preserve the working Phase 5B smoke test, both DAGs, baseline ML checks and regression suite.
    runtime.run()
    before = probe()
    save('before.json', before)
    # Checked-in partitions cannot count as execution evidence: remove only the
    # selected disposable runner outputs, then require their recreation.
    for layer in ['bronze', 'silver', 'gold']:
        for date in DATES:
            (ROOT / 'data' / layer / f'orders_{date}.csv').unlink()
    runtime.container('phase6-unpause', ['airflow', 'dags', 'unpause', 'data_pipeline'])
    backfill(DATES[0], DATES[-1], 'historical-backfill')
    first, runs = wait(lambda r: r['run_type'] == 'backfill', expected=3)
    assert sorted(r['logical_date'][:10] for r in runs) == DATES
    for run in runs:
        date = run['logical_date'][:10]
        assert run['data_interval_start'][:10] == date
        from datetime import date as Date, timedelta
        assert run['data_interval_end'][:10] == (Date.fromisoformat(date) + timedelta(days=1)).isoformat()
        for layer in ['bronze', 'silver', 'gold']:
            assert f'data/{layer}/orders_{date}.csv' in first['files']
    save('historical-backfill.json', {'dates': DATES, 'runs': runs, 'files': first['files']})
    original_ids = {r['run_id'] for r in first['runs']}
    backfill(DATES[0], DATES[0], 'idempotent-backfill')
    second, reruns = wait(lambda r: r['run_id'] not in original_ids and r['run_type'] == 'backfill')
    assert reruns[0]['logical_date'][:10] == DATES[0]
    assert first['files'] == second['files'], 'Row counts, duplicate counts or SHA-256 changed on rerun'
    for layer in ['silver', 'gold']:
        assert second['files'][f'data/{layer}/orders_{DATES[0]}.csv']['duplicate_order_ids'] == 0
    assert all(v == second['files'][k] for k,v in before['files'].items() if k.startswith('data/raw/'))
    # Adjacent dates and every other output must remain unchanged during Phase 6.
    selected = {f'data/{layer}/orders_{date}.csv' for layer in ['bronze','silver','gold'] for date in DATES}
    assert all(v == second['files'][k] for k,v in before['files'].items() if k not in selected)
    save('idempotency.json', {'identical': True, 'no_append': True, 'raw_hashes_unchanged': 46,
         'all_other_partitions_unchanged': True, 'first_runs': runs, 'second_runs': reruns,
         'first': {k:v for k,v in first['files'].items() if k in selected},
         'second': {k:v for k,v in second['files'].items() if k in selected}})
    suffix = os.environ['GITHUB_RUN_ID'] + '-' + os.environ.get('GITHUB_RUN_ATTEMPT', '1')
    retry_id = 'phase6-retry-' + suffix
    trigger(retry_id, 'once')
    retried, retry_runs = wait(lambda r: r['run_id'] == retry_id)
    retry_task = next(t for t in retry_runs[0]['tasks'] if t['task_id'] == TASKS[0])
    assert int(retry_task['try_number']) == 2
    retry_observations = [r for e in TIMELINE for r in e['runs'] if r['run_id'] == retry_id]
    assert any(t['state'] == 'up_for_retry' and t['try_number'] == '1' for r in retry_observations for t in r['tasks'])
    assert any(t['state'] == 'failed' and t['try_number'] == '1' for t in retry_runs[0]['history'])
    save('retry.json', {'run': retry_runs[0], 'first_attempt_failed': True,
         'retry_scheduled_observed': True, 'final_try_number': 2,
         'limitation': 'Airflow records the first failed try in task_instance_history; the live TI transitions to up_for_retry, not terminal failed.'})
    failed_id = 'phase6-exhaust-' + suffix
    trigger(failed_id, 'exhaust')
    exhausted, failed_runs = wait(lambda r: r['run_id'] == failed_id, failed=True)
    tasks = {t['task_id']: t for t in failed_runs[0]['tasks']}
    assert tasks[TASKS[0]]['state'] == 'failed' and int(tasks[TASKS[0]]['try_number']) == 3
    assert all(tasks[t]['state'] == 'upstream_failed' and tasks[t]['pid'] is None and int(tasks[t]['try_number']) == 0 for t in TASKS[1:])
    assert exhausted['files'] == retried['files'], 'Exhausted failure changed artifacts'
    save('failure-propagation.json', {'run': failed_runs[0], 'artifacts_unchanged': True,
         'limitation': 'Airflow sets timestamps when marking upstream_failed. Zero tries and no worker PID establish that downstream tasks never executed.'})
    runtime.container('phase6-pause', ['airflow', 'dags', 'pause', 'data_pipeline'])
    save('final-metadata.json', exhausted)
    save('execution.json', {'status': 'success', 'commit': os.environ.get('GITHUB_SHA'),
        'workflow_run_url': f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}",
        'backfill_dates': DATES, 'retry_run_id': retry_id, 'failure_run_id': failed_id,
        'execution': 'Airflow 3.1 backfill CLI, real scheduler, LocalExecutor, PostgreSQL',
        'limitations': ['CI retry delay is 30 seconds; production remains 5 minutes with retries=2.',
                       'Date partitions are separate orders_YYYY-MM-DD.csv files, preserving the existing layout.']})


if __name__ == '__main__':
    main()
