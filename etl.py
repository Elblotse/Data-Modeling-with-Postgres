"""
This script processes all the song and log files.
Doing that it loads the data from the files,
transforms them into the desired format
and loads them into the star schema in the database.
"""
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import songplay_table_insert, user_table_insert, song_table_insert, \
    artist_table_insert, time_table_insert, song_select


def process_song_file(cur, filepath):
    """
    Processes the song file passed to it and
    writes the needed data into the corresponding tables.
    """
    # open song file
    data_frame = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(data_frame[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        data_frame[['artist_id', 'artist_name', 'artist_location',
            'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the log file passed to it and
    writes the needed data into the corresponding tables.
    """
    # open log file
    data_frame = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    data_frame = data_frame[data_frame.page == 'NextSong']

    # convert timestamp column to datetime
    time_value = pd.to_datetime(data_frame['ts'])

    # insert time data records
    time_data = (time_value, time_value.dt.year, time_value.dt.month,
                 time_value.dt.isocalendar().week, time_value.dt.dayofweek,
                 time_value.dt.day, time_value.dt.hour)

    column_labels = ('timestamp', 'year', 'month', 'week', 'weekday', 'day', 'hour')

    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = data_frame[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in data_frame.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = row[['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']]. \
            append(pd.Series([songid,
                              artistid],
                             index=[
                                 'songid',
                                 'artistid']))
        songplay_data['ts'] = pd.to_datetime(songplay_data['ts'])

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function reads all the files under the given filepath.
    For each file the corresponding processing function is called.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for file in files:
            all_files.append(os.path.abspath(file))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Starting point for the ETL process.
    The chosen files will be transformed into the specified database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=hallo user=hallo password=hallo")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
