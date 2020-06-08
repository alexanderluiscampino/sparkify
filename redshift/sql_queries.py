import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
drop_table_queries = []
for table_name in config['TABLES'].values():
    drop_table_queries.append("DROP TABLE IF EXISTS {TABLENAME};".format(TABLENAME=table_name))
    
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA_S3_PATH = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA_S3_PATH = config.get('S3', 'SONG_DATA')
# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE {TABLENAME} 
(
    event_id INT IDENTITY(0,1) NOT NULL,
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session INTEGER,
    user_last_name VARCHAR(255),
    song_length DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id BIGINT NOT NULL SORTKEY DISTKEY,
    song_title VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(50) NOT NULL,
    user_agent TEXT,
    user_id VARCHAR(100),
    PRIMARY KEY (event_id)
);
""".format(TABLENAME=config['TABLES']['STAGING_EVENTS']))

staging_songs_table_create = ("""
CREATE TABLE {TABLENAME} 
(
    song_id VARCHAR(100) NOT NULL,
    num_songs INTEGER,
    artist_id VARCHAR(100) NOT NULL SORTKEY DISTKEY,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id)
  );
""").format(TABLENAME=config['TABLES']['STAGING_SONGS'])

songplay_table_create = ("""
CREATE TABLE {TABLENAME} 
(
  songplay_id INTEGER IDENTITY(0,1) NOT NULL SORTKEY,
  start_time TIMESTAMP NOT NULL,
  user_id VARCHAR(100) NOT NULL DISTKEY,
  level VARCHAR(10) NOT NULL,
  song_id VARCHAR(40) NOT NULL,
  artist_id VARCHAR(50) NOT NULL,
  session_id VARCHAR(50) NOT NULL,
  location VARCHAR(100),
  user_agent VARCHAR(255)
);
""".format(TABLENAME=config['TABLES']['SONGPLAY']))

user_table_create = ("""
CREATE TABLE {TABLENAME} 
(
  user_id VARCHAR(100) NOT NULL SORTKEY,
  first_name  VARCHAR(50),
  last_name VARCHAR(80),
  gender VARCHAR(10),
  level VARCHAR(10)
    ) diststyle all;
""".format(TABLENAME=config['TABLES']['USERS']))

song_table_create = ("""
CREATE TABLE {TABLENAME} 
(
   song_id VARCHAR(50) NOT NULL SORTKEY,
   title VARCHAR(500) NOT NULL,
   artist_id VARCHAR(50) NOT NULL,
   year INTEGER NOT NULL,
   duration DECIMAL(9) NOT NULL
);
""".format(TABLENAME=config['TABLES']['SONGS']))

artist_table_create = ("""
CREATE TABLE {TABLENAME} 
(
  artist_id   VARCHAR(50) NOT NULL SORTKEY,
  name        VARCHAR(500),
  location    VARCHAR(500),
  latitude    DECIMAL(9),
  longitude   DECIMAL(9)
) diststyle all;
""".format(TABLENAME=config['TABLES']['ARTISTS']))

time_table_create = ("""
CREATE TABLE {TABLENAME} 
(
 start_time  TIMESTAMP NOT NULL SORTKEY,
 hour SMALLINT,
 day SMALLINT,
 week SMALLINT,
 month SMALLINT,
 year SMALLINT,
 weekday SMALLINT
    ) diststyle all;
""".format(TABLENAME=config['TABLES']['TIME']))

# STAGING TABLES

staging_events_copy = ("""
COPY {TABLENAME} FROM '{S3_PATH}'
credentials 'aws_iam_role={ARN}'
format as json '{JSONPATH}'
STATUPDATE ON
region 'us-west-2';
""").format(
    TABLENAME=config['TABLES']['STAGING_EVENTS'], 
    S3_PATH=LOG_DATA_S3_PATH, ARN=ARN, 
    JSONPATH=LOG_JSONPATH
)

staging_songs_copy = ("""
COPY {TABLENAME} FROM '{S3_PATH}'
credentials 'aws_iam_role={ARN}'
format as json 'auto'
ACCEPTINVCHARS AS '^'
STATUPDATE ON
region 'us-west-2';
""").format(
    TABLENAME=config['TABLES']['STAGING_SONGS'], 
    S3_PATH=SONG_DATA_S3_PATH, ARN=ARN
)

# STAR SCHEMA TABLES

songplay_table_insert = ("""INSERT INTO {TABLENAME} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.user_level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM {STAGINGEVENTS} e, {STAGINGSONGS} s
    WHERE e.page = 'NextSong'
    AND e.song_title = s.title
    AND user_id NOT IN (SELECT DISTINCT 
                            s.user_id 
                        FROM {TABLENAME} s 
                        WHERE s.user_id = user_id
                           AND s.start_time = start_time 
                           AND s.session_id = session_id
                           )
""").format(
    TABLENAME=config['TABLES']['SONGPLAY'],
    STAGINGEVENTS=config['TABLES']['STAGING_EVENTS'],
    STAGINGSONGS=config['TABLES']['STAGING_SONGS']
)

user_table_insert = ("""INSERT INTO {TABLENAME} (user_id, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        user_id,
        user_first_name,
        user_last_name,
        user_gender, 
        user_level
    FROM {STAGINGEVENTS}
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM {TABLENAME})
""").format(
    TABLENAME=config['TABLES']['USERS'],
    STAGINGEVENTS=config['TABLES']['STAGING_EVENTS']
)


song_table_insert = ("""INSERT INTO {TABLENAME} (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM {STAGINGSONGS}
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM {TABLENAME})
""").format(
    TABLENAME=config['TABLES']['SONGS'],
    STAGINGSONGS=config['TABLES']['STAGING_SONGS']
)


artist_table_insert = ("""INSERT INTO {TABLENAME} (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM {STAGINGSONGS}
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM {TABLENAME})
""").format(
    TABLENAME=config['TABLES']['ARTISTS'],
    STAGINGSONGS=config['TABLES']['STAGING_SONGS']
)

time_table_insert = ("""INSERT INTO {TABLENAME} (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM {STAGINGEVENTS} s     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM {TABLENAME})
""").format(
    TABLENAME=config['TABLES']['TIME'],
    STAGINGEVENTS=config['TABLES']['STAGING_EVENTS']
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
