import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def convertToPythonDtypes(data):
    '''
    Converts from list (data) possibly containing numpy dtypes to python native dtypes. This is because the .values() method sometimes returns numpy dtypes.

    Returns:
    - List with all values converted to native Python dtypes.
    '''
    convertedData = data
    for ind,i in enumerate(convertedData):
        if isinstance(i, str) == False and isinstance(i, int) == False and isinstance(i, float) == False:
            x = i.item()
            convertedData[ind] = x
        else:
            continue
    return convertedData


def process_song_file(cur, filepath):
    '''
    Processes song file (filepath) JSON log files and inserts using DB cursor (cur).

    Returns:
    None
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = convertToPythonDtypes(df[['song_id', 'title', 'artist_id', 'year', 'duration']].iloc[0,:].values.tolist())
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = convertToPythonDtypes(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].iloc[0,:].values.tolist())
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Processses log file (filepath) JSON log files and inserts using DB cursor (cur).

    Returns:
    None
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df.ts, df.ts.dt.hour, df.ts.dt.day, df.ts.dt.week, df.ts.dt.month, df.ts.dt.year, df.ts.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day','week','month','year','weekday')
    
    # create time_df using dictionary
    d = {}
    for ind, val in enumerate(column_labels):
        d[val] = time_data[ind]
    time_df = pd.DataFrame(d)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
        i += 1


def process_data(cur, conn, filepath, func):
    '''
    Runs processing function (func) on file (filepath) given a DB connection (conn) and cursor (cur).

    Returns:
    None
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
    '''
    Main entrypoint.

    Returns:
    None
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()