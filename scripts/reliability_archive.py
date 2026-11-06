"""Persist bounded, sanitized command and task-log evidence, excluding all secrets."""
import json
from scripts import docker_integration as runtime


def main():
    out = runtime.ROOT / 'reports/phase6'
    out.mkdir(parents=True, exist_ok=True)
    commands = {}
    for path in sorted(runtime.EVIDENCE.glob('*.json')):
        value = json.loads(path.read_text())
        if isinstance(value, dict) and 'exit_code' in value:
            commands[path.stem] = value
    (out / 'command-exits.json').write_text(runtime.redact(json.dumps(commands, indent=2, sort_keys=True)))
    test_log = runtime.EVIDENCE / 'container-tests.log'
    if test_log.exists():
        (out / 'regression-tests.log').write_text(runtime.redact(test_log.read_text()))
    for path in (runtime.EVIDENCE / 'task-logs').rglob('*.log'):
        if 'phase6-' in str(path) or 'phase5b-' in str(path):
            target = out / 'task-logs' / path.relative_to(runtime.EVIDENCE / 'task-logs')
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(runtime.redact(path.read_text()))
    for name in ['task-states-data_pipeline.json', 'task-states-ml_training_pipeline.json',
                 'final-artifact-verification.log']:
        path = runtime.EVIDENCE / name
        if path.exists():
            (out / name).write_text(runtime.redact(path.read_text()))


if __name__ == '__main__':
    main()
