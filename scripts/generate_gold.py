"""Regenerate selected historical partitions using the unchanged ETL functions."""
import argparse
import json
from src import etl


def supplied_dates():
    from datetime import date, timedelta
    return [(date(2026, 10, 1) + timedelta(days=i)).isoformat() for i in range(46)]


def generate(dates=None):
    from dags.data_pipeline import batch_lock
    results = []
    for day in supplied_dates() if dates is None else dates:
        with batch_lock(day):
            etl.extract_to_bronze(day)
            etl.transform_to_silver(day)
            etl.prepare_gold(day)
            results.append(etl.validate_gold(day))
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dates', nargs='+', help='Default: all 46 supplied dates')
    print(json.dumps(generate(parser.parse_args().dates), indent=2))
