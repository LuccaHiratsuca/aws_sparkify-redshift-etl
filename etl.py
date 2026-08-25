"""ETL pipeline: S3 -> Redshift staging tables -> Sparkify star schema.

The pipeline runs in two phases:

1. **Extract & load** -- ``COPY`` the raw JSON event logs and song metadata
   from S3 straight into the staging tables. COPY reads in parallel across
   every slice of the cluster, which is orders of magnitude faster than
   funnelling rows through the leader node with INSERTs.
2. **Transform** -- a set of ``INSERT ... SELECT`` statements reshape the
   staging tables into the fact table and its four dimensions. The transform
   runs entirely inside Redshift, so no warehouse data is ever pulled down to
   the client.

Run ``create_tables.py`` first to (re)create the schema.

Usage:
    python etl.py [--config dwh.cfg] [--skip-copy] [--skip-checks]
"""

import argparse
import sys
import time

import psycopg2

from quality_checks import run_quality_checks
from sql_queries import copy_table_queries, insert_table_queries, table_names
from utils import (
    DEFAULT_CONFIG_PATH,
    connect,
    count_rows,
    load_config,
    run_queries,
)


def load_staging_tables(cur):
    """Bulk load the raw S3 JSON files into the staging tables.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    run_queries(cur, copy_table_queries, "copy")


def insert_tables(cur):
    """Transform the staging tables into the fact and dimension tables.

    ``songplays`` is loaded first because the ``time`` dimension is derived
    from it.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    run_queries(cur, insert_table_queries, "insert")


def report_row_counts(cur):
    """Print the row count of every table in the warehouse.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    print("\nRow counts after load")
    for table in table_names:
        print(f"  {table:<16} {count_rows(cur, table):>8,}")


def vacuum_and_analyze(cur):
    """Reclaim space and refresh planner statistics after the load.

    ``COMPUPDATE OFF`` / ``STATUPDATE OFF`` keep the COPY fast but leave the
    tables unsorted and their statistics stale, which would give the query
    planner bad row estimates. Running VACUUM and ANALYZE once at the end of
    the load is the cheaper way to get both.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    print("\nOptimising tables")
    for table in table_names:
        print(f"  VACUUM + ANALYZE {table} ...", end=" ", flush=True)
        started = time.perf_counter()
        cur.execute(f"VACUUM {table};")
        cur.execute(f"ANALYZE {table};")
        print(f"done in {time.perf_counter() - started:.1f}s")


def parse_args():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to the configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--skip-copy",
        action="store_true",
        help="Reuse the staging tables already in Redshift and only re-run "
             "the transform step (useful while iterating on the INSERTs).",
    )
    parser.add_argument(
        "--skip-checks",
        action="store_true",
        help="Do not run the data quality checks after the load.",
    )
    return parser.parse_args()


def main():
    """Run the full ETL pipeline end to end."""
    args = parse_args()
    config = load_config(args.config)

    started = time.perf_counter()
    conn, cur = connect(config)
    try:
        if args.skip_copy:
            print("Skipping S3 -> staging load (--skip-copy)")
        else:
            print("Loading S3 -> staging tables")
            load_staging_tables(cur)

        print("\nTransforming staging tables -> star schema")
        insert_tables(cur)

        vacuum_and_analyze(cur)
        report_row_counts(cur)

        failed = 0
        if args.skip_checks:
            print("\nSkipping data quality checks (--skip-checks)")
        else:
            print()
            failed = run_quality_checks(cur)
    finally:
        cur.close()
        conn.close()

    print(f"\nETL finished in {time.perf_counter() - started:.1f}s")
    if failed:
        sys.exit(f"{failed} data quality check(s) failed.")
    print("Next: python analytics.py")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, psycopg2.Error) as error:
        sys.exit(f"etl.py failed: {error}")
