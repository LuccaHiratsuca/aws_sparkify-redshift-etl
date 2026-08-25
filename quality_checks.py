"""Data quality checks for the Sparkify data warehouse.

Run after ``etl.py`` to confirm the load produced a warehouse the analytics
team can trust. Three families of checks are applied:

* **Completeness** -- every table has rows (a silent COPY failure shows up as
  an empty staging table).
* **Integrity** -- primary keys are unique and not null, and the fact table's
  foreign keys resolve against the dimensions.
* **Plausibility** -- values fall in ranges the domain allows (hours 0-23,
  non-negative durations, and so on).

Usage:
    python quality_checks.py [--config dwh.cfg]

Exits non-zero if any check fails, so it can gate a downstream job.
"""

import argparse
import sys

import psycopg2

from sql_queries import table_names
from utils import DEFAULT_CONFIG_PATH, connect, load_config

# Each check is (description, sql, expected_value). The SQL must return a
# single scalar, which is compared for equality against expected_value.
CHECKS = [
    # -- Integrity: primary keys ------------------------------------------
    (
        "songplays.songplay_id has no duplicates",
        "SELECT COUNT(*) - COUNT(DISTINCT songplay_id) FROM songplays;",
        0,
    ),
    (
        "users.user_id has no duplicates",
        "SELECT COUNT(*) - COUNT(DISTINCT user_id) FROM users;",
        0,
    ),
    (
        "songs.song_id has no duplicates",
        "SELECT COUNT(*) - COUNT(DISTINCT song_id) FROM songs;",
        0,
    ),
    (
        "artists.artist_id has no duplicates",
        "SELECT COUNT(*) - COUNT(DISTINCT artist_id) FROM artists;",
        0,
    ),
    (
        "time.start_time has no duplicates",
        "SELECT COUNT(*) - COUNT(DISTINCT start_time) FROM time;",
        0,
    ),
    # -- Integrity: not-null fact keys ------------------------------------
    (
        "songplays has no null start_time or user_id",
        "SELECT COUNT(*) FROM songplays "
        "WHERE start_time IS NULL OR user_id IS NULL;",
        0,
    ),
    # -- Integrity: referential --------------------------------------------
    (
        "every songplays.user_id exists in users",
        "SELECT COUNT(*) FROM songplays sp "
        "LEFT JOIN users u ON u.user_id = sp.user_id "
        "WHERE u.user_id IS NULL;",
        0,
    ),
    (
        "every songplays.start_time exists in time",
        "SELECT COUNT(*) FROM songplays sp "
        "LEFT JOIN time t ON t.start_time = sp.start_time "
        "WHERE t.start_time IS NULL;",
        0,
    ),
    (
        "every matched songplays.song_id exists in songs",
        "SELECT COUNT(*) FROM songplays sp "
        "LEFT JOIN songs s ON s.song_id = sp.song_id "
        "WHERE sp.song_id IS NOT NULL AND s.song_id IS NULL;",
        0,
    ),
    (
        "every matched songplays.artist_id exists in artists",
        "SELECT COUNT(*) FROM songplays sp "
        "LEFT JOIN artists a ON a.artist_id = sp.artist_id "
        "WHERE sp.artist_id IS NOT NULL AND a.artist_id IS NULL;",
        0,
    ),
    # -- Correctness of the fact table filter -------------------------------
    (
        "songplays row count matches the NextSong events in staging",
        "SELECT (SELECT COUNT(*) FROM songplays) - "
        "(SELECT COUNT(*) FROM staging_events "
        " WHERE page = 'NextSong' AND userId IS NOT NULL);",
        0,
    ),
    # -- Plausibility -------------------------------------------------------
    (
        "users.level only contains 'free' or 'paid'",
        "SELECT COUNT(*) FROM users WHERE level NOT IN ('free', 'paid');",
        0,
    ),
    (
        "time.hour is within 0-23 and weekday within 0-6",
        "SELECT COUNT(*) FROM time "
        "WHERE hour NOT BETWEEN 0 AND 23 OR weekday NOT BETWEEN 0 AND 6;",
        0,
    ),
    (
        "songs.duration is never negative",
        "SELECT COUNT(*) FROM songs WHERE duration < 0;",
        0,
    ),
    (
        "artists coordinates are within valid lat/long bounds",
        "SELECT COUNT(*) FROM artists "
        "WHERE latitude NOT BETWEEN -90 AND 90 "
        "   OR longitude NOT BETWEEN -180 AND 180;",
        0,
    ),
]


def check_row_counts(cur):
    """Verify that every table in the warehouse contains at least one row.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.

    Returns:
        tuple[int, int]: Number of checks passed and failed.
    """
    passed = failed = 0
    print("Row counts")
    for table in table_names:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        if count > 0:
            print(f"  PASS  {table:<16} {count:>8,} rows")
            passed += 1
        else:
            print(f"  FAIL  {table:<16} {count:>8,} rows -- table is empty")
            failed += 1
    return passed, failed


def check_assertions(cur):
    """Run the integrity and plausibility assertions in ``CHECKS``.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.

    Returns:
        tuple[int, int]: Number of checks passed and failed.
    """
    passed = failed = 0
    print("\nIntegrity and plausibility")
    for description, sql, expected in CHECKS:
        cur.execute(sql)
        actual = cur.fetchone()[0]
        if actual == expected:
            print(f"  PASS  {description}")
            passed += 1
        else:
            print(f"  FAIL  {description} "
                  f"(expected {expected}, got {actual})")
            failed += 1
    return passed, failed


def run_quality_checks(cur):
    """Run every data quality check and summarise the result.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.

    Returns:
        int: The number of failed checks (0 means the warehouse is healthy).
    """
    print("=" * 68)
    print("DATA QUALITY CHECKS")
    print("=" * 68)

    count_passed, count_failed = check_row_counts(cur)
    assert_passed, assert_failed = check_assertions(cur)

    passed = count_passed + assert_passed
    failed = count_failed + assert_failed

    print("-" * 68)
    print(f"{passed} passed, {failed} failed, {passed + failed} total")
    print("=" * 68)
    return failed


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
    """Connect to Redshift and run the data quality suite."""
    args = parse_args()
    config = load_config(args.config)

    conn, cur = connect(config)
    try:
        failed = run_quality_checks(cur)
    finally:
        cur.close()
        conn.close()

    if failed:
        sys.exit(f"{failed} data quality check(s) failed.")


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, psycopg2.Error) as error:
        sys.exit(f"quality_checks.py failed: {error}")
