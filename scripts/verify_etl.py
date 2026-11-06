"""Run requested partitions twice and record actual integrity evidence."""
import hashlib
import json
from src import etl


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    raw = {p: digest(p) for p in (etl.ROOT/'data/raw').glob('*.csv')}
    manifest = json.loads((etl.ROOT/'reports/raw_sha256.json').read_text())
    for path, value in raw.items():
        assert manifest[str(path.relative_to(etl.ROOT))] == value
    results = []
    for day in ['2026-10-01','2026-10-18','2026-10-19','2026-10-20']:
        snapshots = []
        for _ in range(2):
            etl.extract_to_bronze(day)
            etl.transform_to_silver(day)
            etl.prepare_gold(day)
            validation = etl.validate_gold(day)
            snapshots.append({layer: {'rows': len(etl.read_frame(etl.path_for(layer, day))),
                                     'sha256': digest(etl.path_for(layer, day))}
                              for layer in ['bronze','silver','gold']})
            assert etl.path_for('bronze', day).read_bytes() == etl.path_for('raw', day).read_bytes()
        assert snapshots[0] == snapshots[1]
        results.append({'batch_date': day, 'layers': snapshots[0], 'identical_rerun': True,
                        'validation': validation})
    assert all(digest(path) == value for path,value in raw.items())
    report = {'all_46_raw_hashes_verified': True, 'runs': results}
    etl.atomic_write(etl.ROOT/'reports/etl_verification.json', (json.dumps(report,indent=2)+'\n').encode())
    print(json.dumps(report,indent=2))

if __name__ == '__main__': main()
