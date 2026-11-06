"""Evidence handling must not expose local secrets or turn partial runs into success."""
import json
from scripts import docker_integration as ci


def test_redact_credentials_and_runtime_jwt(monkeypatch):
    monkeypatch.setattr(ci, 'secret_values', lambda: ['private-password', 'private-key'])
    value = 'postgresql://airflow:private-password@postgres private-key Bearer abc123 eyJabc.def.ghi'
    sanitized = ci.redact(value)
    assert 'private-password' not in sanitized and 'private-key' not in sanitized
    assert 'abc123' not in sanitized and 'eyJabc' not in sanitized


def test_failed_command_records_exit_code_without_secret(tmp_path, monkeypatch):
    import sys
    monkeypatch.setattr(ci, 'EVIDENCE', tmp_path)
    monkeypatch.setattr(ci, 'secret_values', lambda: ['sensitive'])
    code, _ = ci.execute('failure', [sys.executable, '-c', "print('sensitive'); raise SystemExit(7)"], check=False)
    assert code == 7
    assert json.loads((tmp_path / 'failure.json').read_text())['exit_code'] == 7
    assert 'sensitive' not in (tmp_path / 'failure.log').read_text()
    assert 'sensitive' not in (tmp_path / 'failure.json').read_text()


def test_probe_decoding_uses_observed_json():
    observed = {'state': 'running', 'tasks': {}}
    output = 'Airflow startup log\nRUNTIME_JSON=' + json.dumps(observed)
    assert ci.decode_probe(output) == observed
