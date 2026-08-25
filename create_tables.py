"""Reset the Sparkify data warehouse schema on Redshift.

Drops every staging, fact and dimension table if it exists and recreates them
from scratch, so the script is safe to re-run whenever the ETL pipeline needs a
clean slate.

Usage:
    python create_tables.py [--config dwh.cfg]
"""

import argparse
import sys

import psycopg2

from sql_queries import create_table_queries, drop_table_queries
from utils import DEFAULT_CONFIG_PATH, connect, load_config, run_queries


def drop_tables(cur):
    """Drop all staging, fact and dimension tables if they exist.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    run_queries(cur, drop_table_queries, "drop")


def create_tables(cur):
    """Create the staging tables and the star schema.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
    """
    run_queries(cur, create_table_queries, "create")


def parse_args():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments with a ``config`` attribute.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to the configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    return parser.parse_args()


def main():
    """Connect to Redshift, then drop and recreate the whole schema."""
    args = parse_args()
    config = load_config(args.config)

    conn, cur = connect(config)
    try:
        print("Dropping existing tables")
        drop_tables(cur)
        print("Creating staging tables and star schema")
        create_tables(cur)
    finally:
        cur.close()
        conn.close()

    print("Schema is ready. Next: python etl.py")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, psycopg2.Error) as error:
        sys.exit(f"create_tables.py failed: {error}")
