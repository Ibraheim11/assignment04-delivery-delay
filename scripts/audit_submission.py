"""Read-only submission audit; regenerated data stays in a disposable work directory."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import unquote, urlsplit
import zipfile

from src import etl, train

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_DATES = ['2026-10-01', '2026-10-18', '2026-10-19', '2026-10-20']


def digest(content):
    return hashlib.sha256(content).hexdigest()


def audit():
    result = {}
    manifest = json.loads((ROOT / 'reports/raw_sha256.json').read_text())
    expected = {f'data/raw/orders_{day}.csv' for day in train.supplied_dates()}
    actual = {str(p.relative_to(ROOT)) for p in (ROOT / 'data/raw').glob('*.csv')}
    assert set(manifest) == actual == expected, 'Raw date inventory differs'
    with zipfile.ZipFile(ROOT / 'Assignment04_46_Daily_CSV_Batches.zip') as archive:
        assert archive.testzip() is None, 'Original archive CRC failure'
        assert {n for n in archive.namelist() if n.endswith('.csv')} == expected | {'data_dictionary.csv'}
        for name in sorted(expected):
            content = (ROOT / name).read_bytes()
            assert content == archive.read(name), name
            assert digest(content) == manifest[name], name
        assert (ROOT / 'data_dictionary.csv').read_bytes() == archive.read('data_dictionary.csv')
    result['original_data'] = {'csv_files': 46, 'archive_byte_equality': True,
                               'sha256_manifest_match': True, 'dictionary_byte_equality': True}
    result['examples'] = []
    for day in EXAMPLE_DATES:
        layers = {}
        for layer in ['bronze', 'silver', 'gold']:
            path = ROOT / f'data/{layer}/orders_{day}.csv'
            frame = etl.read_frame(path)
            if layer == 'bronze':
                assert path.read_bytes() == (ROOT / f'data/raw/{path.name}').read_bytes()
            else:
                assert not frame.order_id.duplicated().any()
            if layer == 'gold':
                etl.validate_gold_frame(frame, day)
            layers[layer] = {'rows': len(frame), 'sha256': digest(path.read_bytes())}
        result['examples'].append({'batch_date': day, 'layers': layers})

    # Recreate all dates twice without modifying any submitted CSV or model.
    (ROOT / 'work').mkdir(exist_ok=True)
    original_root = etl.ROOT
    with tempfile.TemporaryDirectory(prefix='submission-audit-', dir=ROOT / 'work') as directory:
        scratch = Path(directory)
        (scratch / 'data/raw').mkdir(parents=True)
        for name in sorted(expected):
            (scratch / name).write_bytes((ROOT / name).read_bytes())
        try:
            etl.ROOT = scratch
            for day in train.supplied_dates():
                snapshots = []
                for _ in range(2):
                    etl.extract_to_bronze(day)
                    etl.transform_to_silver(day)
                    etl.prepare_gold(day)
                    etl.validate_gold(day)
                    snapshots.append({layer: digest(etl.path_for(layer, day).read_bytes())
                                      for layer in ['bronze', 'silver', 'gold']})
                assert snapshots[0] == snapshots[1], day
                if day in EXAMPLE_DATES:
                    for layer, sha in snapshots[0].items():
                        assert sha == digest((ROOT / f'data/{layer}/orders_{day}.csv').read_bytes())
            frame = train.load_gold_partitions(scratch / 'data/gold')
            _, test, split = train.chronological_split(frame)
            metrics = json.loads((ROOT / 'metrics/metrics.json').read_text())
            actual_metrics = train.evaluate_model(ROOT / 'models/delivery_delay_model.joblib', test, split)
            assert actual_metrics == metrics, 'Saved model predictions do not reproduce saved metrics'
        finally:
            etl.ROOT = original_root
    result['regeneration'] = {'validated_dates': 46, 'total_unique_orders': len(frame),
                               'identical_runs_per_date': 2, 'submitted_examples_match': True}
    result['saved_model'] = {'predictions_and_metrics_match': True, 'metrics': metrics}

    result['screenshots'] = []
    for manifest_path in [ROOT / 'reports/phase6/screenshots.json', ROOT / 'reports/phase7/ui-screenshots.json']:
        if not manifest_path.exists():
            continue
        for shot in json.loads(manifest_path.read_text())['screenshots']:
            path = ROOT / shot['path']
            assert path.read_bytes().startswith(b'\x89PNG\r\n\x1a\n'), str(path)
            assert digest(path.read_bytes()) == shot['sha256'], str(path)
            result['screenshots'].append(shot['path'])

    paths = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'],
                                    cwd=ROOT).decode().split('\0')
    paths = sorted(set(p for p in paths if p))
    forbidden = []
    secret_findings = []
    broken_links = []
    local_links = 0
    secret_patterns = [rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
                       rb'\bAKIA[0-9A-Z]{16}\b', rb'\bgh[pousr]_[A-Za-z0-9]{30,}\b',
                       rb'\bgithub_pat_[A-Za-z0-9_]{40,}\b',
                       # Excludes the deliberately short fake JWT in redaction tests.
                       rb'\beyJ[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{20,}\b']
    for name in paths:
        path = ROOT / name
        if (any(part in {'.venv', 'venv', '__pycache__', '.pytest_cache', 'node_modules', 'work', 'logs'}
                for part in path.relative_to(ROOT).parts)
                or path.suffix in {'.db', '.sqlite', '.sqlite3', '.pyc', '.tmp'} or path.name == '.env'):
            forbidden.append(name)
        content = path.read_bytes()
        if any(re.search(pattern, content) for pattern in secret_patterns):
            secret_findings.append(name)  # Never print possible secret values.
        if path.suffix == '.md':
            for target in re.findall(r'!?\[[^\]]*\]\(([^\s)]+)(?:\s+[^)]*)?\)', content.decode()):
                target = target.strip('<>')
                parsed = urlsplit(target)
                if parsed.scheme or parsed.netloc or not parsed.path:
                    continue
                local_links += 1
                if not (path.parent / unquote(parsed.path)).exists():
                    broken_links.append({'source': name, 'target': target})
    assert not forbidden, f'Unnecessary or private runtime files: {forbidden}'
    assert not secret_findings, f'Potential credential material in: {secret_findings}'
    assert not broken_links, f'Broken local Markdown links: {broken_links}'
    result['hygiene'] = {'files_checked': len(paths), 'forbidden_runtime_files': forbidden,
                         'high_confidence_secret_findings': secret_findings,
                         'local_markdown_links_checked': local_links, 'broken_local_links': broken_links,
                         'scope': 'Tracked and nonignored proposed files; pattern scan plus manual review. External URL status and prose claims reviewed separately.'}
    result['status'] = 'passed'
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('work/submission-audit.json'))
    args = parser.parse_args()
    observed = audit()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(observed, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'status': observed['status'], 'raw_files': 46, 'gold_dates': 46,
                      'rows': observed['regeneration']['total_unique_orders'],
                      'screenshots': len(observed['screenshots']), 'output': str(args.output)}, sort_keys=True))
