import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events;"
staging_songs_table_drop ="DROP TABLE IF EXISTS stg_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stg_events(
                event_id    BIGINT IDENTITY(0,1) ,
                artist      VARCHAR              ,
                auth        VARCHAR              ,
                firstName   VARCHAR              ,
                gender      VARCHAR              ,
                itemInSession INTEGER            ,
                lastName    VARCHAR              ,
                length      DECIMAL              ,
                level       VARCHAR              ,
                location    VARCHAR              ,
                method      VARCHAR              ,
                page        VARCHAR              ,
                registration VARCHAR             ,
                sessionId   INTEGER              ,
                song        VARCHAR              ,
                status      INTEGER              ,
                ts          TIMESTAMP            ,
                userAgent   VARCHAR              ,
                userId      INTEGER              

);
""")


staging_songs_table_create = ("""
        CREATE  TABLE IF NOT EXISTS stg_songs(
        num_songs INTEGER   ,
        artist_id CHAR (18) ,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR,
        artist_name VARCHAR ,
        song_id CHAR (18) ,
        title VARCHAR ,
        duration NUMERIC ,
        year INTEGER );
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users(
        user_id             INTEGER         NOT NULL SORTKEY PRIMARY KEY,
        first_name          VARCHAR         NOT NULL,
        last_name           VARCHAR         NOT NULL,
        gender              VARCHAR         NOT NULL,
        level               VARCHAR         NOT NULL);
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS song(
        song_id   VARCHAR sortkey,
        title     VARCHAR         NOT NULL,
        artist_id VARCHAR         NOT NULL,
        year      INTEGER           ,
        duration    DECIMAL);
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artist(
        artist_id   VARCHAR         NOT NULL SORTKEY,
        name        VARCHAR         NULL,
        location    VARCHAR         NULL,
        latitude    DECIMAL         NULL,
        longitude   DECIMAL         NULL
                )diststyle all;
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time(
                start_time  TIMESTAMP  NOT NULL SORTKEY,
                hour        SMALLINT   NULL,
                day         SMALLINT   NULL,
                week        SMALLINT   NULL,
                month       SMALLINT   NULL,
                year        SMALLINT   NULL,
                weekday     SMALLINT   NULL
)diststyle all;
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
        songplay_id BIGINT identity(0, 1) primary key,
        start_time timestamp not null,
        user_id INTEGER not null,
        level varchar not null,
        song_id char NOT NULL(18),
        artist_id char (18) NOT NULL,
        session_id INTEGER not null,
        location VARCHAR,
        user_agent VARCHAR not null    );
""")


# STAGING TABLES

staging_events_copy = ("""copy stg_events from {}
    credentials 'aws_iam_role={}' 
    region 'us-west-2' format as json {}
    timeformat 'epochmillisecs'
    ; 
""").format(LOG_DATA,ARN,LOG_JSONPATH)


staging_songs_copy = ("""copy stg_songs 
                          from {} 
                          credentials 'aws_iam_role={}' 
                          json 'auto';
                      """).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT  DISTINCT se.ts as start_time,
            se.userId        AS user_id, 
            se.level         AS level, 
            ss.song_id       AS song_id, 
            ss.artist_id     AS artist_id, 
            se.sessionId     AS session_id, 
            se.location      AS location, 
            se.userAgent     AS user_agent
    FROM stg_events se
    JOIN stg_songs  ss   ON (se.song = ss.title AND se.artist = ss.artist_name)
    AND se.page  =  'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(se.userId)    AS user_id,
            se.firstName           AS first_name,
            se.lastName            AS last_name,
            se.gender,
            se.level
    FROM stg_events se
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = (""" INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(ss.song_id) AS song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
    FROM stg_songs ss
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(ss.artist_id) AS artist_id,
            ss.artist_name         AS name,
            ss.artist_location     AS location,
            ss.artist_latitude     AS latitude,
            ss.artist_longitude    AS longitude
    FROM stg_songs ss
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
