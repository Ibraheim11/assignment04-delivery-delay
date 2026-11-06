"""Read-only observations from the real Compose metadata DB and fitted artifacts."""
import argparse
import json
from airflow.utils.session import create_session
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.dag import DagModel
from scripts.container_smoke import verify, TASKS


def observe(dag_id, run_id):
    with create_session() as session:
        run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one_or_none()
        tasks = session.query(TaskInstance).filter_by(dag_id=dag_id, run_id=run_id).all()
        return {'dag_id': dag_id, 'run_id': run_id, 'state': run.state if run else 'absent',
                'tasks': {ti.task_id: {'state': ti.state, 'try_number': ti.try_number} for ti in tasks}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['discovery', 'states', 'final'])
    parser.add_argument('--dag-id')
    parser.add_argument('--run-id')
    args = parser.parse_args()
    if args.mode == 'states':
        result = observe(args.dag_id, args.run_id)
    else:
        result = verify(check_gold=args.mode == 'final',
                        baseline='reports/phase4_execution.json' if args.mode == 'final' else None)
        with create_session() as session:
            result['registered'] = {name: session.query(DagModel).filter_by(dag_id=name).count() == 1
                                    for name in TASKS}
    print('RUNTIME_JSON=' + json.dumps(result, sort_keys=True))
