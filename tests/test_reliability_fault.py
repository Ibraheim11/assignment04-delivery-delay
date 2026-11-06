"""Fault safety boundaries; actual retries are verified separately by the scheduler."""
from types import SimpleNamespace
import pytest
from scripts.reliability_fault import inject


def context(attempt=1, mode='once', run_id='phase6-retry-test'):
    return {'dag_run': SimpleNamespace(conf={'phase6_fault': mode}),
            'run_id': run_id, 'ti': SimpleNamespace(try_number=attempt)}


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv('ASSIGNMENT04_RELIABILITY_CI', raising=False)
    inject('extract_to_bronze', context())


def test_ci_fault_is_scoped_and_transient(monkeypatch):
    monkeypatch.setenv('ASSIGNMENT04_RELIABILITY_CI', 'true')
    inject('extract_to_bronze', context(run_id='ordinary-production-run'))
    inject('prepare_gold', context())
    with pytest.raises(RuntimeError, match='attempt=1'):
        inject('extract_to_bronze', context())
    inject('extract_to_bronze', context(attempt=2))
    for attempt in (1,2,3):
        with pytest.raises(RuntimeError, match='mode=exhaust'):
            inject('extract_to_bronze', context(attempt=attempt, mode='exhaust'))
