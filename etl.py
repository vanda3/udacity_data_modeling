import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

    

def process_song_file(cur, filepath):
    '''
        IN 
            * cur:          db reference
            * filepath:     song_data file being processed
        OUT
            * inserted data in songs table
            * inserted data artists table
    '''
    # open song file
    df = pd.DataFrame(pd.read_json(filepath, lines=True, orient='columns'))

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    for song in song_data:
        cur.execute(song_table_insert, song)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    for artist in artist_data:
        cur.execute(artist_table_insert, artist)


def process_log_file(cur, filepath):
    '''
        IN 
            * cur:          db reference
            * filepath:     log_data file being processed
        OUT
            * inserted data in time table
            * inserted data in users table
            * inserted data in songplays table
    '''
    # open log file
    df = pd.DataFrame(pd.read_json(filepath, lines=True, orient='columns'))

    # filter by NextSong action
    df = df[df['page'] == "NextSong"].reset_index()

    # convert timestamp column to datetime
    t = df['ts'].drop_duplicates()
    t = pd.Series(sorted(pd.to_datetime(t,unit='ms')))
    
    # conver
    time_data = [t.dt.strftime('%Y-%m-%d %H:%M:%S'), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.weekofyear.tolist(), t.dt.month.tolist(), t.dt.year, t.dt.weekday.tolist()]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']    
    
    # combine time_data and column_labels in a dictionary
    tmp_dict = dict()
    for n in range(len(column_labels)):
        tmp_dict[column_labels[n]] = time_data[n]
    time_df = pd.DataFrame(tmp_dict)

    # insert time data record
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    user_df = user_df.replace("",None)
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, ('%'+str(row.song)+'%', '%'+str(row.artist)+'%', float(row.length)))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
            print(results)
        else:
            songid, artistid = None, None

        # insert songplay record
        timestamp = pd.to_datetime(row.ts, unit='ms').strftime('%Y-%m-%d %H:%M:%S')
        songplay_data = (timestamp, row.userId, row.level, str(songid), str(artistid), row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
        IN 
            * cur:          db reference
            * conn:         db parameters (host, dbname, user, password)
            * filepath:     log_data file being processed
            * func:         function called (process_log_file or process_song_file)
        OUT
            * processed all data files
            * inserted all data into tables
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
    
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()