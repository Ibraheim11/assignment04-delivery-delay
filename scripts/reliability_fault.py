"""Controlled, opt-in faults before ETL writes; never edits input CSVs."""
import os


def inject(stage, context):
    if os.environ.get('ASSIGNMENT04_RELIABILITY_CI') != 'true':
        return
    run = context['dag_run']
    mode = (run.conf or {}).get('phase6_fault')
    if not mode:
        return
    if not context['run_id'].startswith('phase6-') or stage != 'extract_to_bronze':
        return
    if mode not in ('once', 'exhaust'):
        raise ValueError('Unknown Phase 6 CI fault mode')
    attempt = context['ti'].try_number
    if mode == 'exhaust' or attempt == 1:
        raise RuntimeError(f'PHASE6_CONTROLLED_TRANSIENT_FAILURE mode={mode} attempt={attempt}')
