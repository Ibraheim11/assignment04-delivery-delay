"""Capture unmodified live Airflow UI pages; never save authentication screens/tokens."""
import hashlib
import json
from pathlib import Path
from urllib.parse import quote
from playwright.sync_api import sync_playwright
from scripts import docker_integration as runtime

ROOT = runtime.ROOT
OUT = ROOT / 'screenshots/phase6'
REPORT = ROOT / 'reports/phase6'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.mkdir(parents=True, exist_ok=True)
    result = {'screenshots': [], 'limitations': []}
    try:
        env = dict(line.split('=', 1) for line in (ROOT / '.env').read_text().splitlines() if '=' in line)
        with sync_playwright() as p:
            browser = p.chromium.launch()
            context = browser.new_context(viewport={'width': 1600, 'height': 1100})
            # Authenticate against the real FAB endpoint, then let the Airflow UI
            # consume its supported JWT cookie. Never persist the response/storage.
            response = context.request.post('http://127.0.0.1:8080/auth/token', data={
                'username': env.get('AIRFLOW_ADMIN_USER', 'admin'),
                'password': env['AIRFLOW_ADMIN_PASSWORD']})
            page = context.new_page()
            if response.ok:
                token = response.json()['access_token']
                context.add_cookies([{'name': '_token', 'value': token,
                                     'url': 'http://127.0.0.1:8080/', 'httpOnly': False}])
                result['authentication'] = 'FAB token endpoint'
            else:
                # The pinned FAB provider may fail its FastAPI login route.
                # Try the genuine Flask login form before declaring a limitation.
                result['token_login_http_status'] = response.status
                page.goto('http://127.0.0.1:8080/auth/login/', wait_until='networkidle')
                page.locator('input[name="username"]').wait_for(timeout=15000)
                page.locator('input[name="username"]').fill(env.get('AIRFLOW_ADMIN_USER', 'admin'))
                page.locator('input[name="password"]').fill(env['AIRFLOW_ADMIN_PASSWORD'])
                page.locator('input[type="submit"],button[type="submit"]').first.click()
                page.wait_for_load_state('networkidle')
                result['authentication'] = 'FAB live login form'
            def capture_page(name, route, required, graph=False):
                page.goto('http://127.0.0.1:8080' + route, wait_until='networkidle')
                page.wait_for_timeout(2500)
                if page.locator('input[type="password"]').count():
                    raise RuntimeError('Authentication did not complete; no login screenshot saved')
                if graph:
                    button = page.get_by_role('button', name=__import__('re').compile('Show Graph', __import__('re').I))
                    if button.count():
                        button.first.click()
                    page.locator('.react-flow__node').first.wait_for(timeout=15000)
                for value in required:
                    page.get_by_text(value, exact=False).first.wait_for(timeout=15000)
                body = page.locator('body').inner_text()
                if any(secret in body for secret in runtime.secret_values()):
                    raise RuntimeError('Secret detected in rendered page; screenshot refused')
                path = OUT / (name + '.png')
                page.screenshot(path=str(path), full_page=True)
                result['screenshots'].append({'path': str(path.relative_to(ROOT)), 'url': page.url,
                    'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            def capture(name, route, required, graph=False):
                try:
                    capture_page(name, route, required, graph)
                except Exception as exc:
                    result['limitations'].append(name + ': ' + runtime.redact(str(exc)))
            capture('both-dags', '/dags', ['data_pipeline', 'ml_training_pipeline'])
            capture('data-pipeline-graph', '/dags/data_pipeline', ['extract_to_bronze','validate_gold'], True)
            capture('ml-training-graph', '/dags/ml_training_pipeline', ['load_gold_data','save_model'], True)
            capture('historical-backfills', '/dags/data_pipeline/backfills', ['Backfill'])
            metadata_path = REPORT / 'final-metadata.json'
            if metadata_path.exists():
                runs = json.loads(metadata_path.read_text())['runs']
                backfill = next(r for r in runs if r['run_type'] == 'backfill' and r['logical_date'].startswith('2026-10-19'))
                route = '/dags/data_pipeline/runs/' + quote(backfill['run_id'], safe='')
                capture('backfill-successful-tasks', route, ['success', 'extract_to_bronze'])
                retry = next(r for r in runs if r['run_id'].startswith('phase6-retry-'))
                route = '/dags/data_pipeline/runs/' + quote(retry['run_id'], safe='') + '/tasks/extract_to_bronze'
                capture('retry-final-success-logs', route, ['success'])
                capture('retry-first-failed-attempt-logs', route + '?try_number=1', ['PHASE6_CONTROLLED_TRANSIENT_FAILURE'])
                rerun = next(r for r in runs if r['run_type'] == 'backfill' and r['logical_date'].startswith('2026-10-18'))
                route = '/dags/data_pipeline/runs/' + quote(rerun['run_id'], safe='') + '/tasks/validate_gold'
                capture('same-date-rerun-task-history', route, ['success', 'Task Tries'], True)
            else:
                result['limitations'].append('Reliability execution did not reach final metadata; partial UI evidence only.')
            ml_path = runtime.EVIDENCE / 'task-states-ml_training_pipeline.json'
            if ml_path.exists():
                ml = json.loads(ml_path.read_text())
                if ml['state'] != 'success':
                    raise RuntimeError('ML run did not succeed; no successful ML screenshot claimed')
                route = '/dags/ml_training_pipeline/runs/' + quote(ml['run_id'], safe='')
                capture('ml-successful-task-graph', route, ['success', 'load_gold_data', 'save_model'], True)
                capture('ml-evaluation-task-logs', route + '/tasks/evaluate_model', ['success', 'evaluation='])
            else:
                result['limitations'].append('ML scheduler run metadata unavailable; no ML task-log capture.')
            browser.close()
    except Exception as exc:
        result['limitations'].append(runtime.redact(str(exc)))
        # Keep the real server diagnosis if UI authentication is unavailable.
        _, output = runtime.execute('ui-auth-diagnostics', runtime.COMPOSE +
                                    ['logs', '--no-color', '--tail', '150', 'airflow-apiserver'], check=False)
        (REPORT / 'ui-server-diagnostics.log').write_text(runtime.redact(output))
    (REPORT / 'screenshots.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
