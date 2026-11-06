"""Create local Compose secrets and writable output directories without overwriting .env."""
import base64
import os
from pathlib import Path
import secrets


def setup(root=None):
    root = Path(root) if root else Path(__file__).resolve().parents[1]
    path = root / '.env'
    if path.exists():
        raise FileExistsError('.env already exists; keep its secrets and edit it explicitly')
    uid = os.getuid() if hasattr(os, 'getuid') else 50000
    if uid == 0:
        uid = 50000  # Airflow services should not run as root.
    content = f'AIRFLOW_UID={uid}\nAIRFLOW_ADMIN_USER=admin\n'
    content += ''.join(f'{key}={secrets.token_hex(32)}\n' for key in
                       ['POSTGRES_PASSWORD', 'AIRFLOW_ADMIN_PASSWORD', 'AIRFLOW_JWT_SECRET'])
    content += f'AIRFLOW_FERNET_KEY={base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()}\n'
    # O_EXCL also protects an existing file if two setup processes race.
    with open(path, 'x', opener=lambda p, flags: os.open(p, flags, 0o600)) as handle:
        handle.write(content)
    for name in ['logs', 'data/bronze', 'data/silver', 'data/gold', 'data/ml', 'data/.locks', 'models', 'metrics', 'reports/etl']:
        (root / name).mkdir(parents=True, exist_ok=True)
    print('Created .env and output directories. Keep .env private; no secrets printed.')


if __name__ == '__main__':
    setup()
