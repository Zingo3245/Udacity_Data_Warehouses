import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
itemInSesson int,
lastName varchar,
length float,
level varchar,
location varchar, 
method varchar,
page varchar,
registration float,
sessionId int NOT NULL,
song varchar,
status int,
ts timestamp NOT NULL,
userAgent varchar,
userId int NOT NULL PRIMARY KEY
)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs int, 
song_id varchar PRIMARY KEY NOT NULL,
title varchar,
year int, 
duration float,
artist_id varchar NOT NULL,
artist_name varchar, 
artist_location varchar, 
artist_latitude double precision, 
artist_longitude double precision,
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
songplay_id serial PRIMARY KEY NOT NULL, 
start_time timestamp NOT NULL, 
user_id int NOT NULL, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id varchar, 
location varchar, 
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user
(
user_id int PRIMARY KEY NOT NULL, 
firstName varchar, 
lastName varchar, 
gender varchar, 
level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(
song_id varchar PRIMARY KEY NOT NULL, 
title varchar, 
artist_id varchar NOT NULL, 
year int, 
duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(
artist_id varchar PRIMARY KEY NOT NULL, 
name varchar, 
location varchar, 
latitude double precision, 
longitude double precision
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time timestamp PRIMARY KEY NOT NULL, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::654825254714:user/airflow_redshift_user'
GZIP REGION 'us-east-1'
""").format()

staging_songs_copy = (""" COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::654825254714:user/airflow_redshift_user'
GZIP REGION 'us-east-1'
""").format()

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT ts, user_id, level, song_id, artist_id, session_id, location FROM staging_events se JOIN (SELECT song_id, artist_id, name, title FROM song s JOIN artist a ON s.artist_id=a.artist_id) sa ON se.artist=sa.name AND se.song=sa.title
WHERE page='NextSong'
""")

user_table_insert = ("""INSERT INTO user(user_id, first_name , last_name, gender, level) SELECT DISTINCT user_id, first_name, last_name, gender, level FROM staging_events WHERE page = 'NextSong' WHERE user_id NOT IN (SELECT DISTINCT user_id FROM user)
""")

song_table_insert = ("""INSERT INTO song(song_id, title, artist_id, year, duration) SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs WHERE song_id NOT IN (SELECT DISTINCT song_id FROM song) 
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude) SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude FROM staging_songs WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artist)
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) SELECT start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time), EXTRACT(MONTH FROM start_time), EXTRACT(YEAR FROM start_time), FROM (SELECT TIMESTAMP 'epoch' + start_time / 1000 * INTERVAL'1 second' AS start_time FROM songplay)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
