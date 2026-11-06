"""Check live FAB login, authenticated DAG API and UI route; never persist tokens."""
import json
from pathlib import Path
import urllib.request


def verify():
    root = Path(__file__).resolve().parents[1]
    env = dict(line.split('=', 1) for line in (root / '.env').read_text().splitlines() if '=' in line)
    # Only local Compose traffic: do not send development login data to a proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request('http://127.0.0.1:8080/auth/token',
        data=json.dumps({'username': env.get('AIRFLOW_ADMIN_USER', 'admin'),
                         'password': env['AIRFLOW_ADMIN_PASSWORD']}).encode(),
        headers={'Content-Type': 'application/json'})
    with opener.open(request, timeout=30) as response:
        token = json.load(response)['access_token']
    request = urllib.request.Request('http://127.0.0.1:8080/api/v2/dags',
                                    headers={'Authorization': 'Bearer ' + token})
    with opener.open(request, timeout=30) as response:
        dags = {d['dag_id'] for d in json.load(response)['dags']}
    assert {'data_pipeline', 'ml_training_pipeline'} <= dags
    request = urllib.request.Request('http://127.0.0.1:8080/dags',
                                    headers={'Cookie': '_token=' + token})
    with opener.open(request, timeout=30) as response:
        assert response.status == 200 and '/auth/login' not in response.url
        assert 'text/html' in response.headers.get('Content-Type', '')
    result = {'fab_token_login': 'passed', 'authenticated_dag_api': 'passed',
              'authenticated_ui_html_route': 'passed', 'required_dags': sorted(dags),
              'scope': 'Live HTTP integration; no browser rendering or new screenshot claimed.'}
    path = root / 'work/docker-integration/authentication.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    verify()
