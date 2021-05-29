# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays (songplay_id serial, start_time timestamp, user_id integer, level varchar(4), song_id integer, artist_id integer, session_id integer, location text, user_agent text)")

user_table_create = ("CREATE TABLE IF NOT EXISTS users (user_id integer, first_name text, last_name text, gender char(1), level varchar(4))")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id text, title text, artist_id text, year integer, duration real)")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id text, name text, location text, latitude real, longitude real)")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour integer, day integer, week integer, month integer, year integer, weekday integer)")

# INSERT RECORDS
songplay_table_insert = ("INSERT INTO songplays (songplay_id, start_time, user_id, level, session_id, location, user_agent, song_id, artist_id) "\
                         "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ('INSERT INTO users (user_id, first_name, last_name, gender, level) '\
                     'VALUES (%s, %s, %s, %s, %s)')

song_table_insert = ('INSERT INTO songs (song_id, title, artist_id, year, duration) '\
                     'VALUES (%s, %s, %s, %s, %s)')

artist_table_insert = ('INSERT INTO artists (artist_id, name, location, latitude, longitude) '\
                       'VALUES (%s, %s, %s, %s, %s)')


time_table_insert = ("INSERT INTO time (start_time, year, month, week, weekday, day, hour) "\
                     "VALUES (%s, %s, %s, %s, %s, %s, %s)")
# FIND SONGS

song_select = ("SELECT song_id, a.artist_id FROM songs s JOIN artists a ON (s.artist_id = a.artist_id) WHERE s.title = %s AND a.name = %s AND s.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]