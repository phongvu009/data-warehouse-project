import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF  EXISTS users"
song_table_drop = "DROP TABLE IF  EXISTS songs"
artist_table_drop ="DROP TABLE IF  EXISTS artists"
time_table_drop = "DROP TABLE IF  EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
        CREATE TABLE staging_events(
                id              INTEGER NOT NULL,
                artist_name     VARCHAR(50) NOT NULL,
                auth            BOOLEAN ,
                first_name      VARCHAR(25) NOT NULL,
                gender          VARCHAR(10) NOT NULL,
                item_in_session INTEGER NOT NULL,
                last_name       VARCHAR(25) NOT NULL,
                length          DECIMAL(8,6) NOT NULL,
                level           VARCHAR(5) NOT NULL,
                location        VARCHAR(50) NOT NULL,
                method          VARCHAR(10) NOT NULL,
                page            VARCHAR(10) NOT NULL,
                registration    DECIMAL(8,6) NOT NULL,
                session_id      INTEGER NOT NULL,
                status          INTEGER NOT NULL,
                ts              TIMESTAMP NOT NULL,
                userAgent       VARCHAR(50) NOT NULL,
                user_id         INTEGER NOT NULL
        )       
""")
staging_songs_table_create = ("""
        CREATE TABLE staging_songs(
                num_songs               INTEGER NOT NULL,
                artist_id               INTEGER NOT NULL,
                artist_latitude         DECIMAL(8,6) NOT NULL,
                artist_longtitude       DECIMAL(9,6) NOT NULL,
                artist_location         VARCHAR(25) NOT NULL,
                artist_name             VARCHAR(50) NOT NULL,
                song_id                 INTEGER NOT NULL,
                title                   VARCHAR(25) NOT NULL,
                duration                DECIMAL(8,6) NOT NULL,
                year                    INTEGER NOT NULL
        )
""")

songplay_table_create = ("""
        CREATE TABLE songplays(
            songplay_id         INTEGER IDENTITY(0,1) NOT NULL,
            start_time          INTEGER NOT NULL,
            user_id             INTEGER NOT NULL, 
            level               VARCHAR(5) NOT NULL,
            song_id             INTEGER NOT NULL distkey,
            artist_id           INTEGER NOT NULL,
            session_id          INTEGER NOT NULL,
            location            VARCHAR(50) NOT NULL,
            user_agent          VARCHAR(50) NOT NULL
        )
""")

user_table_create = ("""
        CREATE TABLE users(
            user_id     INTEGER NOT NULL sortkey,
            first_name  VARCHAR(25) NOT NULL,
            last_name   VARCHAR(25) NOT NULL,
            gender      VARCHAR(10) NOT NULL,
            level       VARCHAR(5) NOT NULL
        )diststyle auto;
""")

song_table_create = ("""
        CREATE TABLE songs(
            song_id     INTEGER     NOT NULL sortkey distkey,
            title       VARCHAR(50) NOT NULL,
            artist_id   INTEGER     NOT NULL,
            year        INTEGER     NOT NULL,
            duration    DECIMAL(8,6)    NOT NULL
        );
""")

artist_table_create = ("""
        CREATE TABLE artists(
            artist_id   INTEGER NOT NULL sortkey,
            artist_name VARCHAR(50) NOT NULL,
            location    VARCHAR(25) NOT NULL,
            latitude   DECIMAL(8,6) NOT NULL,
            longitude   DECIMAL(9,6) NOT NULL 
        )diststyle auto;
""")

time_table_create = ("""
        CREATE TABLE time(
            stat_time    INTEGER NOT NULL sortkey,
            hour         VARCHAR(10) NOT NULL
            day         VARCHAR(10) NOT NULL,
            week        VARCHAR(10) NOT NULL,
            month       VARCHAR(10) NOT NULL,
            year        INTEGER NOT NULL,
            weekday     VARCHAR(1) NOT NULL
        )diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
        COPY staging_events from {}
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON {} 
        REGION 'us-west-2';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
        copy staging_songs from {} 
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 'auto'
        REGION 'us-west-2';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO (start_time , user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
               DISTINCT ( TO_CHAR(start_time :: DATE, 'yyyyMMDD')::integer)  as start date,'
                se.user_id as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.session_id as session_id,
                se.location as location,
                se.user_agent as user_agent
        FROM staging_events se
        JOIN staging_songs ss ON (se.artist_name = ss.artist_name) 

""")

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT user_id,
                first_name,
                last_name,
                gender,
                level
        FROM staging_events 
""")

song_table_insert = ("""
        INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT song_id,
                title,
                artist_id,
                duration
        FROM staging_songs
""")

artist_table_insert = ("""
        INSERT INTO artists(artist_id, artist_name, location, latitude, longtitude)
        SELECT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longtitude
        FROM staging_songs
""")

time_table_insert = ("""
        INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT 
                DISTINCT(TO_CHAR(start_time :: DATE, 'yyyyMMDD')::integer) as start_time
                EXTRACT (HOUR FROM start_time) as hour
                EXTRACT (DAY FROM start_time) as day
                EXTRACT (HOUR FROM start_time) as week
                EXTRACT (HOUR FROM start_time) as month
                EXTRACT (HOUR FROM start_time) as year
                EXTRACT (ISODOW FROM start_time) as weekday

        FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
