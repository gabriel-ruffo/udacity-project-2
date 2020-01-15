import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE','ARN')[1:-1]
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events_table (
                                 artist varchar,
                                 auth varchar,
                                 firstName varchar,
                                 gender varchar,
                                 itemInSession int,
                                 lastName varchar,
                                 length float,
                                 level varchar,
                                 location varchar,
                                 method varchar,
                                 page varchar,
                                 registration float,
                                 sessionId int,
                                 song varchar,
                                 status int,
                                 ts bigint,
                                 userAgent varchar,
                                 userId int);""")

staging_songs_table_create = ("""CREATE TABLE staging_songs_table (
                                 num_songs int,
                                 artist_id varchar,
                                 artist_latitude float(6),
                                 artist_longitude float(6),
                                 artist_location varchar,
                                 artist_name varchar,
                                 song_id varchar,
                                 title varchar,
                                 duration float,
                                 year int);""")

songplay_table_create = ("""CREATE TABLE songplay_table (
                            songplay_id int IDENTITY(0,1) PRIMARY KEY,
                            start_time bigint,
                            user_id int NOT NULL,
                            level varchar NOT NULL,
                            song_id varchar,
                            artist_id varchar,
                            session_id int NOT NULL,
                            location varchar,
                            user_agent varchar);""")

user_table_create = ("""CREATE TABLE user_table (
                        user_id int PRIMARY KEY, 
                        first_name varchar, 
                        last_name varchar, 
                        gender varchar, 
                        level varchar NOT NULL);""")

song_table_create = ("""CREATE TABLE song_table (
                        song_id varchar PRIMARY KEY, 
                        title varchar, 
                        artist_id varchar NOT NULL, 
                        year int, 
                        duration float);""")

artist_table_create = ("""CREATE TABLE artist_table (
                          artist_id varchar PRIMARY KEY, 
                          name varchar, 
                          location varchar, 
                          latitude float(6), 
                          longitude float(6));""")

time_table_create = ("""CREATE TABLE time_table (
                        start_time bigint PRIMARY KEY, 
                        hour int, 
                        day int, 
                        week int, 
                        month int, 
                        year int, 
                        weekday int);""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events_table from {}
                          CREDENTIALS 'aws_iam_role={}'
                          region 'us-west-2'
                          FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs_table FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         region 'us-west-2'
                         json 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                SELECT ev.ts, ev.userid, ev.level, so.song_id, so.artist_id, ev.sessionid, ev.location, ev.useragent
                                FROM staging_events_table as ev
                                JOIN staging_songs_table as so
                                ON so.artist_name = ev.artist
                                AND so.title = ev.song
                                AND so.duration = ev.length
                                WHERE ev.page = 'NextSong';""")

"""
Idea to use "DISTINCT" instead of "ON CONFLICT" -- because Redshift doesn't accept "ON CONFLICT" -- credit to:
https://knowledge.udacity.com/questions/39503
"""
user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                            SELECT DISTINCT userid, firstname, lastname, gender, level
                            FROM staging_events_table
                            WHERE userid IS NOT NULL;""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration)
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM staging_songs_table
                            WHERE song_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
                              SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                              FROM staging_songs_table
                              WHERE artist_id IS NOT NULL;""")

"""
Method of extracting date values from unix epoch time credit to:
https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
"""
time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday) 
                            SELECT DISTINCT
                                start_time,
                                EXTRACT (hour FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as hour,
                                EXTRACT (day FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as day,
                                EXTRACT (week FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as week,
                                EXTRACT (month FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as month,
                                EXTRACT (year FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as year,
                                EXTRACT (dow FROM (TIMESTAMP 'epoch' + start_time * INTERVAL '1 Second ')) as weekday
                            FROM songplay_table
                            WHERE start_time IS NOT NULL;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]