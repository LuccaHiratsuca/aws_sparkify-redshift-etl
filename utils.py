"""Shared helpers for the Sparkify ETL scripts.

Keeps configuration loading, connection handling and query execution in one
place so ``create_tables.py``, ``etl.py``, ``analytics.py`` and
``quality_checks.py`` all behave (and log) consistently.
"""

import configparser
import time

import psycopg2

DEFAULT_CONFIG_PATH = "dwh.cfg"


def load_config(config_path=DEFAULT_CONFIG_PATH):
    """Read an ini-style config file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        configparser.ConfigParser: The parsed configuration.

    Raises:
        FileNotFoundError: If the file does not exist or is unreadable.
    """
    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise FileNotFoundError(
            f"Could not read '{config_path}'. Copy dwh.cfg, fill in the "
            f"cluster credentials and try again."
        )
    return config


def connect(config):
    """Open a connection to the Redshift cluster described by the config.

    Args:
        config (configparser.ConfigParser): Config with a [CLUSTER] section.

    Returns:
        tuple: The open ``psycopg2`` connection and its cursor.

    Raises:
        ValueError: If required [CLUSTER] values are still blank.
    """
    cluster = config["CLUSTER"]
    missing = [key for key in ("HOST", "DB_NAME", "DB_USER", "DB_PASSWORD",
                               "DB_PORT") if not cluster.get(key)]
    if missing:
        raise ValueError(
            f"Missing [CLUSTER] settings in the config file: "
            f"{', '.join(missing)}. Run `python iac.py create` first, or fill "
            f"them in by hand."
        )

    conn = psycopg2.connect(
        host=cluster["HOST"],
        dbname=cluster["DB_NAME"],
        user=cluster["DB_USER"],
        password=cluster["DB_PASSWORD"],
        port=cluster["DB_PORT"],
    )
    # Autocommit keeps each DDL/COPY/INSERT independent: a failure part-way
    # through a load leaves the previously loaded tables intact instead of
    # silently rolling everything back inside one long transaction.
    conn.set_session(autocommit=True)
    print(f"Connected to {cluster['HOST']}:{cluster['DB_PORT']}"
          f"/{cluster['DB_NAME']}")
    return conn, conn.cursor()


def run_queries(cur, queries, stage):
    """Execute a list of SQL statements in order, timing each one.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
        queries (list[str]): SQL statements, executed sequentially.
        stage (str): Label used in the progress output.

    Raises:
        psycopg2.Error: Re-raised after logging which statement failed.
    """
    total = len(queries)
    stage_start = time.perf_counter()

    for index, query in enumerate(queries, start=1):
        label = summarize(query)
        print(f"  [{stage} {index}/{total}] {label} ...", end=" ", flush=True)
        started = time.perf_counter()
        try:
            cur.execute(query)
        except psycopg2.Error as error:
            print("FAILED")
            print(f"    {error}")
            raise
        print(f"done in {time.perf_counter() - started:.1f}s")

    print(f"  {stage}: {total} statement(s) in "
          f"{time.perf_counter() - stage_start:.1f}s")


def summarize(query, width=52):
    """Collapse a SQL statement into a single short line for logging.

    Args:
        query (str): The SQL statement.
        width (int): Maximum length of the returned label.

    Returns:
        str: A one-line, truncated version of the statement.
    """
    flat = " ".join(query.split())
    return flat if len(flat) <= width else f"{flat[:width - 3]}..."


def count_rows(cur, table):
    """Return the number of rows in a table.

    Args:
        cur (psycopg2.extensions.cursor): Cursor to execute against.
        table (str): Table name.

    Returns:
        int: The row count.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table};")
    return cur.fetchone()[0]
