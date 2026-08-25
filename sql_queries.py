"""SQL statements for the Sparkify data warehouse.

The module is organised in the same order as the pipeline executes:

1. ``drop_table_queries``    -- tear the schema down so runs are idempotent.
2. ``create_table_queries``  -- staging tables + star schema.
3. ``copy_table_queries``    -- bulk load S3 -> Redshift staging tables.
4. ``insert_table_queries``  -- staging tables -> fact and dimension tables.
5. ``analytic_queries``      -- example business questions for the star schema.

Everything is plain SQL text so ``create_tables.py`` and ``etl.py`` stay thin
orchestrators with no embedded SQL of their own.
"""

import configparser

config = configparser.ConfigParser()
config.read("dwh.cfg")

ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get("DWH", "REGION")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# ---------------------------------------------------------------------------
# DROP TABLES
# ---------------------------------------------------------------------------
# Dimensions are dropped after the fact table purely for readability; Redshift
# does not enforce foreign keys, so drop order is not actually constrained.

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# ---------------------------------------------------------------------------
# CREATE STAGING TABLES
# ---------------------------------------------------------------------------
# Staging tables mirror the raw JSON one-to-one: no keys, no constraints, no
# type coercion beyond what COPY needs. They are landing zones, so they use
# DISTSTYLE EVEN to spread the load evenly across slices and keep COPY fast.
#
# IMPORTANT: the column order of staging_events must match the order of the
# paths in log_json_path.json, because COPY ... FORMAT AS JSON <jsonpaths>
# maps by position, not by name.

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR(512),
    auth            VARCHAR(32),
    firstName       VARCHAR(128),
    gender          CHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR(128),
    length          NUMERIC(12, 5),
    level           VARCHAR(16),
    location        VARCHAR(512),
    method          VARCHAR(16),
    page            VARCHAR(64),
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR(512),
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR(512),
    userId          INTEGER
)
DISTSTYLE EVEN;
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR(32),
    artist_latitude     NUMERIC(10, 5),
    artist_longitude    NUMERIC(10, 5),
    artist_location     VARCHAR(512),
    artist_name         VARCHAR(512),
    song_id             VARCHAR(32),
    title               VARCHAR(512),
    duration            NUMERIC(12, 5),
    year                INTEGER
)
DISTSTYLE EVEN;
"""


# ---------------------------------------------------------------------------
# CREATE STAR SCHEMA
# ---------------------------------------------------------------------------
# Distribution strategy
# ---------------------
# songplays / songs : DISTKEY(song_id). The fact table's most expensive join is
#                     songplays -> songs, and songs is the only dimension too
#                     large to replicate. Sharing a distribution key makes that
#                     join local to each slice (no data redistribution).
# users / artists    : DISTSTYLE ALL. Small dimensions (~10^2 and ~10^4 rows),
#   / time             so replicating them to every node removes the broadcast
#                      step from every join at a negligible storage cost.
#
# Sort keys follow the query pattern: songplays and time are almost always
# filtered or grouped by start_time, so it is the sort key on both.

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER         IDENTITY(0, 1)  PRIMARY KEY,
    start_time      TIMESTAMP       NOT NULL        SORTKEY,
    user_id         INTEGER         NOT NULL,
    level           VARCHAR(16),
    song_id         VARCHAR(32)                     DISTKEY,
    artist_id       VARCHAR(32),
    session_id      INTEGER,
    location        VARCHAR(512),
    user_agent      VARCHAR(512)
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER         NOT NULL    PRIMARY KEY    SORTKEY,
    first_name  VARCHAR(128),
    last_name   VARCHAR(128),
    gender      CHAR(1),
    level       VARCHAR(16)
)
DISTSTYLE ALL;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR(32)     NOT NULL    PRIMARY KEY    DISTKEY,
    title       VARCHAR(512)    NOT NULL,
    artist_id   VARCHAR(32)                                SORTKEY,
    year        INTEGER,
    duration    NUMERIC(12, 5)
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR(32)     NOT NULL    PRIMARY KEY    SORTKEY,
    name        VARCHAR(512)    NOT NULL,
    location    VARCHAR(512),
    latitude    NUMERIC(10, 5),
    longitude   NUMERIC(10, 5)
)
DISTSTYLE ALL;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP   NOT NULL    PRIMARY KEY    SORTKEY,
    hour        SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL,
    week        SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    year        SMALLINT    NOT NULL,
    weekday     SMALLINT    NOT NULL
)
DISTSTYLE ALL;
"""


# ---------------------------------------------------------------------------
# COPY (S3 -> staging)
# ---------------------------------------------------------------------------
# COPY is the only sane way to ingest at this volume: it loads in parallel from
# every slice, whereas row-by-row INSERTs would serialise through the leader
# node. The event logs need a JSONPaths file because their keys are camelCase
# and do not match the column names; the song files map cleanly with 'auto'.

staging_events_copy = f"""
COPY staging_events
FROM '{LOG_DATA}'
IAM_ROLE '{ARN}'
REGION '{REGION}'
FORMAT AS JSON '{LOG_JSONPATH}'
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF
STATUPDATE OFF;
"""

staging_songs_copy = f"""
COPY staging_songs
FROM '{SONG_DATA}'
IAM_ROLE '{ARN}'
REGION '{REGION}'
FORMAT AS JSON 'auto'
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF
STATUPDATE OFF;
"""


# ---------------------------------------------------------------------------
# INSERT (staging -> star schema)
# ---------------------------------------------------------------------------

# The grain of songplays is "one row per NextSong event", so the event log
# drives the query and staging_songs is joined in with a LEFT JOIN: a play must
# survive even when the song is missing from the (partial) song dataset.
# Matching on title + artist name alone is ambiguous for re-releases and live
# versions, so duration is used as a tie-breaker with a 2 second tolerance to
# absorb rounding differences between the two datasets.
songplay_table_insert = """
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id,
    session_id, location, user_agent
)
SELECT
    TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second'   AS start_time,
    e.userId                                                AS user_id,
    e.level                                                 AS level,
    s.song_id                                               AS song_id,
    s.artist_id                                             AS artist_id,
    e.sessionId                                             AS session_id,
    e.location                                              AS location,
    e.userAgent                                             AS user_agent
FROM staging_events e
LEFT JOIN staging_songs s
       ON e.song = s.title
      AND e.artist = s.artist_name
      AND ABS(e.length - s.duration) < 2
WHERE e.page = 'NextSong'
  AND e.userId IS NOT NULL;
"""

# A user appears once per event, and their subscription level changes over
# time. ROW_NUMBER keeps exactly one row per user_id -- the most recent event --
# so `level` reflects the user's current plan instead of an arbitrary one.
user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT user_id, first_name, last_name, gender, level
FROM (
    SELECT
        userId      AS user_id,
        firstName   AS first_name,
        lastName    AS last_name,
        gender      AS gender,
        level       AS level,
        ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS recency_rank
    FROM staging_events
    WHERE page = 'NextSong'
      AND userId IS NOT NULL
) ranked_users
WHERE recency_rank = 1;
"""

# Song metadata is repeated across files, but a song_id always carries the same
# attributes, so a plain DISTINCT is enough to deduplicate.
song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
"""

# Unlike songs, the same artist_id can arrive with different location/lat/long
# (some files leave them blank), so DISTINCT would emit several rows per
# artist. ROW_NUMBER picks the richest record: the one that actually has
# coordinates and a location.
artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, name, location, latitude, longitude
FROM (
    SELECT
        artist_id           AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude,
        ROW_NUMBER() OVER (
            PARTITION BY artist_id
            ORDER BY
                CASE WHEN artist_latitude IS NOT NULL THEN 0 ELSE 1 END,
                CASE WHEN artist_location IS NOT NULL THEN 0 ELSE 1 END
        ) AS completeness_rank
    FROM staging_songs
    WHERE artist_id IS NOT NULL
) ranked_artists
WHERE completeness_rank = 1;
"""

# time is derived from the fact table rather than from staging_events so that
# it contains exactly the timestamps that songplays references -- a conformed
# dimension with no orphan rows. songplays is already loaded at this point.
time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(hour    FROM start_time)    AS hour,
    EXTRACT(day     FROM start_time)    AS day,
    EXTRACT(week    FROM start_time)    AS week,
    EXTRACT(month   FROM start_time)    AS month,
    EXTRACT(year    FROM start_time)    AS year,
    EXTRACT(dayofweek FROM start_time)  AS weekday
FROM (SELECT DISTINCT start_time FROM songplays) distinct_times;
"""


# ---------------------------------------------------------------------------
# ANALYTICS -- example questions the star schema was designed to answer
# ---------------------------------------------------------------------------
# Consumed by analytics.py. Each entry is (question, sql).

most_played_songs = """
SELECT
    s.title                 AS song,
    a.name                  AS artist,
    COUNT(*)                AS plays
FROM songplays sp
JOIN songs   s ON s.song_id   = sp.song_id
JOIN artists a ON a.artist_id = sp.artist_id
GROUP BY s.title, a.name
ORDER BY plays DESC, song ASC
LIMIT 10;
"""

busiest_hours = """
SELECT
    t.hour                                  AS hour_of_day,
    COUNT(*)                                AS plays,
    COUNT(DISTINCT sp.user_id)              AS distinct_listeners
FROM songplays sp
JOIN time t ON t.start_time = sp.start_time
GROUP BY t.hour
ORDER BY plays DESC
LIMIT 10;
"""

top_locations = """
SELECT
    sp.location                             AS location,
    COUNT(*)                                AS plays,
    COUNT(DISTINCT sp.user_id)              AS distinct_listeners
FROM songplays sp
GROUP BY sp.location
ORDER BY plays DESC
LIMIT 10;
"""

paid_vs_free = """
SELECT
    sp.level                                            AS subscription,
    COUNT(*)                                            AS plays,
    COUNT(DISTINCT sp.user_id)                          AS users,
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sp.user_id), 1)
                                                        AS plays_per_user
FROM songplays sp
GROUP BY sp.level
ORDER BY plays DESC;
"""

most_active_users = """
SELECT
    u.user_id                               AS user_id,
    u.first_name || ' ' || u.last_name      AS name,
    u.level                                 AS level,
    COUNT(*)                                AS plays,
    COUNT(DISTINCT sp.session_id)           AS sessions
FROM songplays sp
JOIN users u ON u.user_id = sp.user_id
GROUP BY u.user_id, u.first_name, u.last_name, u.level
ORDER BY plays DESC
LIMIT 10;
"""

weekday_vs_weekend = """
SELECT
    CASE WHEN t.weekday IN (0, 6) THEN 'weekend' ELSE 'weekday' END AS day_type,
    COUNT(*)                                                        AS plays,
    COUNT(DISTINCT sp.user_id)                                      AS listeners
FROM songplays sp
JOIN time t ON t.start_time = sp.start_time
GROUP BY 1
ORDER BY plays DESC;
"""

top_artists = """
SELECT
    a.name                                  AS artist,
    COUNT(*)                                AS plays,
    COUNT(DISTINCT sp.song_id)              AS distinct_songs_played
FROM songplays sp
JOIN artists a ON a.artist_id = sp.artist_id
GROUP BY a.name
ORDER BY plays DESC
LIMIT 10;
"""


# ---------------------------------------------------------------------------
# QUERY LISTS -- the public interface of this module
# ---------------------------------------------------------------------------

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy,
]

# songplays must be inserted before time, which is derived from it.
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]

analytic_queries = [
    ("Which songs are played the most?", most_played_songs),
    ("Which artists are played the most?", top_artists),
    ("What are the peak listening hours of the day?", busiest_hours),
    ("Where are our listeners?", top_locations),
    ("How does engagement differ between paid and free users?", paid_vs_free),
    ("Who are our most active users?", most_active_users),
    ("Do people listen more on weekdays or weekends?", weekday_vs_weekend),
]

# Tables to report row counts for after a load, in dependency order.
table_names = [
    "staging_events",
    "staging_songs",
    "songplays",
    "users",
    "songs",
    "artists",
    "time",
]
