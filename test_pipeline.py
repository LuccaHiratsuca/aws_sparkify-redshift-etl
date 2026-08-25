"""Offline tests for the Sparkify pipeline.

These run without an AWS account or a live Redshift cluster: they check the
invariants that would otherwise only surface as a confusing runtime failure
half an hour into a load -- a mismatched JSONPaths column order, an
unresolved S3 placeholder in a COPY, a table that is created but never
dropped.

Usage:
    python -m unittest discover -v
"""

import configparser
import os
import shutil
import tempfile
import unittest

import analytics
import iac
import quality_checks
import sql_queries
import utils

# The order of the paths in s3://udacity-dend/log_json_path.json. COPY with a
# JSONPaths file maps by position, so staging_events must declare its columns
# in exactly this order or the data lands in the wrong columns.
LOG_JSONPATH_ORDER = [
    "artist", "auth", "firstname", "gender", "iteminsession", "lastname",
    "length", "level", "location", "method", "page", "registration",
    "sessionid", "song", "status", "ts", "useragent", "userid",
]


def column_names(create_statement):
    """Extract the declared column names from a CREATE TABLE statement.

    Args:
        create_statement (str): A CREATE TABLE statement.

    Returns:
        list[str]: Lower-cased column names, in declaration order.
    """
    body = create_statement[
        create_statement.index("(") + 1:create_statement.rindex(")")
    ]
    return [
        line.strip().split()[0].lower()
        for line in body.strip().splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]


class TestStagingSchema(unittest.TestCase):
    """The staging tables must line up with the raw files they load."""

    def test_staging_events_matches_jsonpath_order(self):
        self.assertEqual(
            column_names(sql_queries.staging_events_table_create),
            LOG_JSONPATH_ORDER,
        )

    def test_staging_songs_declares_every_song_field(self):
        self.assertEqual(
            column_names(sql_queries.staging_songs_table_create),
            [
                "num_songs", "artist_id", "artist_latitude",
                "artist_longitude", "artist_location", "artist_name",
                "song_id", "title", "duration", "year",
            ],
        )


class TestCopyStatements(unittest.TestCase):
    """COPY statements are f-strings, so check nothing was left unresolved."""

    def test_no_unresolved_placeholders(self):
        for statement in sql_queries.copy_table_queries:
            self.assertNotIn("{", statement)

    def test_event_copy_uses_the_jsonpaths_file(self):
        self.assertIn(sql_queries.LOG_DATA, sql_queries.staging_events_copy)
        self.assertIn(
            sql_queries.LOG_JSONPATH, sql_queries.staging_events_copy
        )

    def test_song_copy_uses_auto_mapping(self):
        self.assertIn(sql_queries.SONG_DATA, sql_queries.staging_songs_copy)
        self.assertIn("FORMAT AS JSON 'auto'", sql_queries.staging_songs_copy)


class TestSchemaConsistency(unittest.TestCase):
    """Every table must be creatable, droppable and countable."""

    def test_every_table_has_a_create_and_a_drop(self):
        creates = " ".join(sql_queries.create_table_queries)
        for table in sql_queries.table_names:
            with self.subTest(table=table):
                self.assertIn(f"IF NOT EXISTS {table} ", creates)
                self.assertIn(
                    f"DROP TABLE IF EXISTS {table};",
                    sql_queries.drop_table_queries,
                )

    def test_songplays_is_loaded_before_time(self):
        """time is derived from songplays, so ordering is load-critical."""
        inserts = sql_queries.insert_table_queries
        self.assertLess(
            inserts.index(sql_queries.songplay_table_insert),
            inserts.index(sql_queries.time_table_insert),
        )

    def test_fact_and_song_share_a_distribution_key(self):
        """Co-locating the fact-to-song join is the core design decision."""
        self.assertIn("song_id", sql_queries.songplay_table_create)
        self.assertIn("DISTKEY", sql_queries.songplay_table_create)
        self.assertIn("DISTKEY", sql_queries.song_table_create)

    def test_small_dimensions_are_replicated(self):
        for statement in (
            sql_queries.user_table_create,
            sql_queries.artist_table_create,
            sql_queries.time_table_create,
        ):
            with self.subTest(statement=utils.summarize(statement)):
                self.assertIn("DISTSTYLE ALL", statement)


class TestQualityChecks(unittest.TestCase):
    """The check suite should be well-formed before it ever hits a cluster."""

    def test_checks_are_triples_returning_a_scalar(self):
        self.assertGreater(len(quality_checks.CHECKS), 0)
        for description, sql, expected in quality_checks.CHECKS:
            with self.subTest(check=description):
                self.assertTrue(description)
                self.assertIn("SELECT", sql.upper())
                self.assertIsInstance(expected, int)

    def test_descriptions_are_unique(self):
        descriptions = [check[0] for check in quality_checks.CHECKS]
        self.assertCountEqual(descriptions, set(descriptions))


class TestAnalyticsRendering(unittest.TestCase):
    """The report must render both flavours, including the empty case."""

    headers = ["song", "artist", "plays"]
    rows = [("You're The One", "Dwight Yoakam", 37)]

    def test_plain_render(self):
        output = analytics.render(self.headers, self.rows)
        self.assertIn("Dwight Yoakam", output)
        self.assertNotIn("|", output)

    def test_markdown_render(self):
        output = analytics.render(self.headers, self.rows, markdown=True)
        self.assertIn("|", output)
        self.assertIn("Dwight Yoakam", output)

    def test_empty_render(self):
        self.assertEqual(analytics.render(self.headers, []), "(no rows)")

    def test_every_question_has_a_query(self):
        for question, sql in sql_queries.analytic_queries:
            with self.subTest(question=question):
                self.assertTrue(question.endswith("?"))
                self.assertIn("FROM songplays", sql)


class TestConfigHandling(unittest.TestCase):
    """Config loading fails loudly, and write-back is non-destructive."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.path = os.path.join(self.directory, "dwh.cfg")
        shutil.copy("dwh.cfg", self.path)

    def tearDown(self):
        shutil.rmtree(self.directory)

    def read(self):
        parsed = configparser.ConfigParser()
        parsed.read(self.path)
        return parsed

    def test_write_back_sets_the_value(self):
        iac.update_config_value(self.path, "CLUSTER", "HOST", "dwh.aws.com")
        self.assertEqual(self.read().get("CLUSTER", "HOST"), "dwh.aws.com")

    def raw(self):
        with open(self.path, encoding="utf-8") as handle:
            return handle.read()

    def test_write_back_preserves_comments_and_siblings(self):
        before = self.raw().count(";")
        iac.update_config_value(self.path, "IAM_ROLE", "ARN", "arn:aws:iam::1:role/r")
        self.assertEqual(self.raw().count(";"), before)
        self.assertEqual(self.read().get("CLUSTER", "DB_PORT"), "5439")
        self.assertEqual(
            self.read().get("S3", "LOG_DATA"), "s3://udacity-dend/log_data"
        )

    def test_write_back_can_clear_a_value(self):
        iac.update_config_value(self.path, "CLUSTER", "HOST", "dwh.aws.com")
        iac.update_config_value(self.path, "CLUSTER", "HOST", "")
        self.assertEqual(self.read().get("CLUSTER", "HOST"), "")

    def test_write_back_reports_an_unknown_key(self):
        self.assertFalse(
            iac.update_config_value(self.path, "CLUSTER", "NOPE", "x")
        )

    def test_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            utils.load_config(os.path.join(self.directory, "absent.cfg"))

    def test_blank_cluster_settings_raise_before_connecting(self):
        with self.assertRaises(ValueError) as caught:
            utils.connect(utils.load_config(self.path))
        self.assertIn("HOST", str(caught.exception))


class TestSummarize(unittest.TestCase):
    """Log labels must be single-line and bounded."""

    def test_collapses_whitespace(self):
        self.assertEqual(utils.summarize("SELECT\n  1 ;"), "SELECT 1 ;")

    def test_truncates_long_statements(self):
        label = utils.summarize("SELECT " + "x" * 200)
        self.assertLessEqual(len(label), 52)
        self.assertTrue(label.endswith("..."))


if __name__ == "__main__":
    unittest.main(verbosity=2)
