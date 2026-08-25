"""Print an analytics report from the Sparkify star schema.

Runs the example business questions defined in ``sql_queries.analytic_queries``
against the warehouse and renders each result as a table. This is the payoff of
the whole pipeline: proof that the schema answers the questions the analytics
team actually asks.

Usage:
    python analytics.py [--config dwh.cfg] [--markdown]
"""

import argparse
import sys

import psycopg2
from tabulate import tabulate

from sql_queries import analytic_queries
from utils import DEFAULT_CONFIG_PATH, connect, load_config


def fetch(cur, sql):
    """Execute a query and return its column names and rows.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
        sql (str): The query to run.

    Returns:
        tuple[list[str], list[tuple]]: Column headers and result rows.
    """
    cur.execute(sql)
    headers = [column.name for column in cur.description]
    return headers, cur.fetchall()


def render(headers, rows, markdown=False):
    """Format a result set for the terminal or for a Markdown document.

    Args:
        headers (list[str]): Column names.
        rows (list[tuple]): Result rows.
        markdown (bool): Render a Markdown table instead of a plain one.

    Returns:
        str: The formatted table.
    """
    if not rows:
        return "(no rows)"
    return tabulate(
        rows,
        headers=headers,
        tablefmt="github" if markdown else "simple",
        floatfmt=",.1f",
        intfmt=",",
    )


def report(cur, queries=analytic_queries, markdown=False):
    """Run every analytics query and print the results.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
        queries (list[tuple[str, str]]): (question, sql) pairs to run.
        markdown (bool): Render results as Markdown tables.
    """
    title = "SPARKIFY SONG PLAY ANALYTICS"
    if markdown:
        print(f"## {title.title()}")
    else:
        print("=" * 72)
        print(title)
        print("=" * 72)

    for number, (question, sql) in enumerate(queries, start=1):
        heading = f"{number}. {question}"
        if markdown:
            print(f"\n### {heading}\n")
        else:
            print(f"\n{heading}")
            print("-" * len(heading))
        headers, rows = fetch(cur, sql)
        print(render(headers, rows, markdown=markdown))

    print()


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
        "--markdown",
        action="store_true",
        help="Render the report as Markdown, ready to paste into the README.",
    )
    return parser.parse_args()


def main():
    """Connect to Redshift and print the analytics report."""
    args = parse_args()
    config = load_config(args.config)

    conn, cur = connect(config)
    try:
        report(cur, markdown=args.markdown)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError, psycopg2.Error) as error:
        sys.exit(f"analytics.py failed: {error}")
