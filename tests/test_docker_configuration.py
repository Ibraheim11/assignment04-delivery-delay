"""Deployment contracts checked without claiming a successful container runtime."""
import base64
from pathlib import Path

import pytest
import yaml
from scripts.setup_docker_env import setup

ROOT = Path(__file__).resolve().parents[1]
CONFIG = yaml.safe_load((ROOT / 'docker-compose.yml').read_text())
SERVICES = CONFIG['services']
AIRFLOW = [name for name in SERVICES if name.startswith('airflow-')]


def test_supported_architecture_and_dependencies():
    assert {'postgres', 'airflow-init', 'airflow-apiserver', 'airflow-scheduler',
            'airflow-dag-processor', 'airflow-triggerer'} <= set(SERVICES)
    assert SERVICES['airflow-scheduler']['environment']['AIRFLOW__CORE__EXECUTOR'] == 'LocalExecutor'
    for name in AIRFLOW:
        service = SERVICES[name]
        assert service['depends_on']['postgres']['condition'] == 'service_healthy'
        if name != 'airflow-init':
            assert service['depends_on']['airflow-init']['condition'] == 'service_completed_successfully'
    assert SERVICES['airflow-scheduler']['depends_on']['airflow-apiserver']['condition'] == 'service_healthy'
    for name in ['postgres', 'airflow-apiserver', 'airflow-scheduler', 'airflow-dag-processor', 'airflow-triggerer']:
        assert SERVICES[name]['healthcheck']['retries'] >= 5


def test_shared_paths_locking_and_localhost_exposure():
    expected = {f'./{name}:/opt/airflow/{name}' for name in
                ['dags', 'src', 'scripts', 'data', 'models', 'metrics', 'reports', 'logs']}
    for name in AIRFLOW:
        service = SERVICES[name]
        volumes = {v.removesuffix(':ro') for v in service['volumes']}
        assert expected <= volumes
        assert './data/raw:/opt/airflow/data/raw:ro' in service['volumes']
        assert service['environment']['PYTHONPATH'] == '/opt/airflow'
        assert service['user'] == '${AIRFLOW_UID:-50000}:0'
        assert not service.get('privileged', False)
    assert SERVICES['airflow-apiserver']['ports'] == ['127.0.0.1:8080:8080']
    assert all('ports' not in service for name, service in SERVICES.items() if name != 'airflow-apiserver')
    assert 'postgres-db:/var/lib/postgresql/data' in SERVICES['postgres']['volumes']


def test_secrets_required_and_safe_defaults():
    env = SERVICES['airflow-scheduler']['environment']
    assert ':?' in env['AIRFLOW__API_AUTH__JWT_SECRET']
    assert ':?' in env['AIRFLOW__CORE__FERNET_KEY']
    assert env['AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION'] == 'true'
    assert env['AIRFLOW__CORE__LOAD_EXAMPLES'] == 'false'
    assert env['AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS'] == 'false'
    assert 'airflow-apiserver' in env['NO_PROXY']
    assert SERVICES['airflow-init']['environment']['_AIRFLOW_DB_MIGRATE'] == 'true'
    for line in (ROOT / '.env.example').read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            key, value = line.split('=', 1)
            if key not in ['AIRFLOW_UID', 'AIRFLOW_ADMIN_USER']:
                assert not value


def test_image_preserves_airflow_and_model_versions():
    dockerfile = (ROOT / 'Dockerfile').read_text()
    assert 'FROM apache/airflow:slim-3.1.0-python3.12 AS runtime' in dockerfile
    assert dockerfile.index('USER airflow') < dockerfile.index('pip install')
    assert '"apache-airflow==3.1.0"' in dockerfile
    assert 'pip check' in dockerfile
    assert 'src.train' not in dockerfile and 'generate_gold' not in dockerfile
    assert (ROOT / '.dockerignore').read_text().splitlines()[1] == '**'
    from scripts.container_smoke import EXPECTED
    pins = (ROOT / 'requirements.txt').read_text()
    for name in ['pandas', 'numpy', 'scikit-learn', 'joblib']:
        assert f'{name}=={EXPECTED[name]}' in pins


def test_setup_creates_secrets_and_refuses_overwrite(tmp_path):
    setup(tmp_path)
    path = tmp_path / '.env'
    before = path.read_bytes()
    env = dict(line.split('=', 1) for line in path.read_text().splitlines())
    assert len(base64.urlsafe_b64decode(env['AIRFLOW_FERNET_KEY'])) == 32
    assert len(env['AIRFLOW_JWT_SECRET']) == 64
    assert path.stat().st_mode & 0o077 == 0
    assert (tmp_path / 'logs').is_dir()
    with pytest.raises(FileExistsError): setup(tmp_path)
    assert path.read_bytes() == before
