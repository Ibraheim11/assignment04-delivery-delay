"""GitHub-runner orchestration. Evidence contains observed results, never credentials."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / 'work/docker-integration'
COMPOSE = ['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'work/compose.ci.yml']
SERVICES = ['postgres', 'airflow-apiserver', 'airflow-scheduler', 'airflow-dag-processor', 'airflow-triggerer']


def secret_values():
    path = ROOT / '.env'
    if not path.exists(): return []
    return [v for line in path.read_text().splitlines() if '=' in line
            for k, v in [line.split('=', 1)] if v and k not in ['AIRFLOW_UID', 'AIRFLOW_ADMIN_USER']]


def redact(text):
    for value in secret_values(): text = text.replace(value, '[REDACTED]')
    text = re.sub(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', '[REDACTED_JWT]', text)
    return re.sub(r'(?i)(Bearer\s+)[A-Za-z0-9._~-]+', r'\1[REDACTED]', text)


def write(name, value):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    text = value if isinstance(value, str) else json.dumps(value, indent=2, sort_keys=True)
    (EVIDENCE / name).write_text(redact(text))


def execute(name, args, timeout=300, check=True):
    print(f'Running {name}', flush=True)
    try:
        result = subprocess.run(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, timeout=timeout)
        output, code = result.stdout, result.returncode
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout or b''
        if isinstance(output, bytes): output = output.decode(errors='replace')
        output += f'\nTimed out after {timeout} seconds\n'
        code = 124
    write(name + '.log', output)
    write(name + '.json', {'command': args, 'exit_code': code})
    print(f'{name}: exit_code={code}', flush=True)
    if check and code:
        print(redact(output[-5000:]), flush=True)
        raise RuntimeError(f'{name} failed with exit code {code}')
    return code, output


def container(name, args, **kwargs):
    return execute(name, COMPOSE + ['exec', '-T', 'airflow-scheduler'] + args, **kwargs)


def decode_probe(output):
    return json.loads(next(line[len('RUNTIME_JSON='):] for line in reversed(output.splitlines())
                           if line.startswith('RUNTIME_JSON=')))


def resources():
    execute('docker-version', ['docker', 'version'])
    execute('docker-info', ['docker', 'info', '--format',
                           'driver={{.Driver}} cpus={{.NCPU}} memory={{.MemTotal}}'])
    execute('memory', ['free', '-h'])
    execute('disk-before', ['df', '-h'])
    execute('docker-disk-before', ['docker', 'system', 'df'])
    gib = 1024**3
    free = shutil.disk_usage(ROOT).free
    if free < 20*gib and os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted':
        # Only disposable SDKs on THIS fresh hosted runner; no Docker/project pruning.
        execute('remove-unused-runner-sdks', ['sudo', 'rm', '-rf', '/usr/local/lib/android', '/opt/ghc'])
        free = shutil.disk_usage(ROOT).free
    ram = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                   if line.startswith('MemTotal:'))) * 1024
    write('resource-check.json', {'free_bytes': free, 'ram_bytes': ram,
                                'required_free_bytes': 20*gib, 'required_ram_bytes': 4*gib})
    execute('disk-after-resource-check', ['df', '-h'])
    if free < 20*gib or ram < 4*gib:
        raise RuntimeError('Runner requires at least 20 GiB free disk and 4 GiB RAM before building')


def health():
    execute('compose-status', COMPOSE + ['ps', '--all', '--format', 'json'])
    observations = {}
    for service in SERVICES:
        _, cid = execute('id-' + service, COMPOSE + ['ps', '-q', service])
        _, output = execute('health-' + service, ['docker', 'inspect', '--format', '{{json .State}}', cid.strip()])
        state = json.loads(output)
        observations[service] = state
        if state.get('Health', {}).get('Status') != 'healthy':
            raise RuntimeError(f'{service} is not healthy')
    write('health-summary.json', observations)


def wait_for_run(dag, run_id, timeout=600):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        _, output = container('states-' + dag, ['python', '-m', 'scripts.airflow_runtime_probe',
                             'states', '--dag-id', dag, '--run-id', run_id])
        observed = decode_probe(output)
        write('task-states-' + dag + '.json', observed)
        if observed['state'] == 'success':
            from scripts.container_smoke import TASKS
            if set(observed['tasks']) != set(TASKS[dag]) or any(t['state'] != 'success' for t in observed['tasks'].values()):
                raise RuntimeError('Successful DagRun lacks four successful task instances')
            return
        if observed['state'] == 'failed' or any(t['state'] in ['failed', 'upstream_failed'] for t in observed['tasks'].values()):
            raise RuntimeError(f'{dag} has failed task instances')
        time.sleep(10)
    raise TimeoutError(f'{dag} did not finish within {timeout}s')


def run():
    resources()
    from scripts.setup_docker_env import setup
    setup(ROOT)
    # GitHub masks secrets too; files additionally pass through redact() before upload.
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        for value in secret_values(): print('::add-mask::'+value, flush=True)
    (ROOT / 'work/compose.ci.yml').write_text('''services:
  airflow-scheduler:
    environment:
      AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE: 'false'
''')
    if os.environ.get('ASSIGNMENT04_RELIABILITY_CI') == 'true':
        import yaml
        override = {'services': {name: {'environment': {
            'AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS': 'true',
            'ASSIGNMENT04_RELIABILITY_CI': 'true',
        }} for name in ['airflow-init', 'airflow-apiserver', 'airflow-scheduler',
                        'airflow-dag-processor', 'airflow-triggerer', 'airflow-cli']}}
        override['services']['airflow-scheduler']['environment']['AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE'] = 'false'
        (ROOT / 'work/compose.ci.yml').write_text(yaml.safe_dump(override))
    # Disables automatic daily runs only in CI; externally triggered runs still use
    # the real scheduler, LocalExecutor and Execution API. Production DAG is unchanged.
    execute('compose-config', COMPOSE + ['config', '--quiet'])
    execute('docker-build', COMPOSE + ['build'], timeout=900)
    execute('postgres-start', COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '180', 'postgres'])
    execute('airflow-init', COMPOSE + ['up', '--no-deps', '--exit-code-from', 'airflow-init', 'airflow-init'], timeout=300)
    execute('services-start', COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '300'] + SERVICES[1:], timeout=360)
    health()
    deadline = time.monotonic()+240
    while True:
        code, output = container('dag-discovery-probe', ['python', '-m', 'scripts.airflow_runtime_probe', 'discovery'], check=False)
        if not code and all(decode_probe(output)['registered'].values()): break
        if time.monotonic() > deadline: raise TimeoutError('Both DAGs were not discovered/registered')
        time.sleep(10)
    container('airflow-version', ['airflow', 'version'])
    container('dag-discovery', ['airflow', 'dags', 'list', '--local'])
    container('dag-import-errors', ['airflow', 'dags', 'list-import-errors', '--local'])
    suffix = os.environ.get('GITHUB_RUN_ID', str(int(time.time()))) + '-' + os.environ.get('GITHUB_RUN_ATTEMPT', '1')
    for dag, conf in [('data_pipeline', {'batch_date': '2026-10-18'}), ('ml_training_pipeline', {})]:
        if dag == 'ml_training_pipeline':
            container('generate-all-gold', ['python', '-m', 'scripts.generate_gold'], timeout=180)
        run_id = 'phase5b-' + dag + '-' + suffix
        container('unpause-' + dag, ['airflow', 'dags', 'unpause', dag])
        container('trigger-' + dag, ['airflow', 'dags', 'trigger', dag, '--run-id', run_id, '--conf', json.dumps(conf)])
        wait_for_run(dag, run_id)
        container('pause-' + dag, ['airflow', 'dags', 'pause', dag])
    container('final-artifact-verification', ['python', '-m', 'scripts.airflow_runtime_probe', 'final'])
    # Original checked-in artifacts remain in Git. Runner outputs are evidence only.
    write('docker-generated-metrics.json', (ROOT / 'metrics/metrics.json').read_text())
    shutil.copyfile(ROOT / 'models/delivery_delay_model.joblib', EVIDENCE / 'delivery_delay_model.joblib')
    # Run the complete existing suite in a separate container/SQLite DB, never in
    # the service PostgreSQL database or scheduler process.
    execute('test-image-build', ['docker', 'build', '--target', 'test', '-t', 'assignment04-airflow:test', '.'], timeout=600)
    (ROOT / 'work/compose.tests.yml').write_text('''services:
  airflow-cli:
    image: assignment04-airflow:test
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////tmp/airflow-test.db
      ASSIGNMENT04_RELIABILITY_CI: 'false'
''')
    execute('container-tests', COMPOSE + ['-f', 'work/compose.tests.yml', 'run', '--rm', '--no-deps',
            'airflow-cli', 'bash', '-c',
            'airflow db migrate && airflow dags reserialize && RUN_AIRFLOW_INTEGRATION=1 python -m pytest -q -o cache_dir=/tmp/pytest-cache'], timeout=360)
    write('result.json', {'status': 'success', 'execution': 'real scheduler + LocalExecutor + Execution API',
                          'commit': os.environ.get('GITHUB_SHA'), 'run_id': os.environ.get('GITHUB_RUN_ID')})


def collect():
    if (ROOT / 'work/compose.ci.yml').exists() and (ROOT / '.env').exists():
        execute('final-compose-status', COMPOSE + ['ps', '--all', '--format', 'json'], check=False)
        execute('compose-service-logs', COMPOSE + ['logs', '--no-color'], check=False)
        for service in SERVICES:
            code, cid = execute('final-id-' + service, COMPOSE + ['ps', '-aq', service], check=False)
            if not code and cid.strip():
                execute('final-health-' + service, ['docker', 'inspect', '--format',
                        '{{json .State}}', cid.strip()], check=False)
        # Copy only text task logs, redacting secrets and JWTs before persistence.
        for path in (ROOT / 'logs').rglob('*.log'):
            relative = path.relative_to(ROOT / 'logs')
            target = EVIDENCE / 'task-logs' / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(redact(path.read_text(errors='replace')))
        execute('shutdown', COMPOSE + ['down', '--volumes', '--remove-orphans'], check=False)
    execute('docker-disk-final', ['docker', 'system', 'df'], check=False)
    if not (EVIDENCE / 'result.json').exists():
        write('result.json', {'status': 'failed_or_incomplete', 'commit': os.environ.get('GITHUB_SHA'),
                              'note': 'Inspect recorded command exit codes and logs; no runtime success inferred.'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['run', 'collect'])
    args = parser.parse_args()
    os.chdir(ROOT)
    # Supports both python scripts/docker_integration.py and python -m scripts...
    import sys
    sys.path.insert(0, str(ROOT))
    if args.mode == 'run': run()
    else: collect()
