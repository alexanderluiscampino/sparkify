import os
import glob
import psycopg2
import pandas as pd
import json
from io import StringIO
import logging
import datetime

from postgre import Postgre

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def get_json_data(filepath):
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for file in files :
            with open(os.path.abspath(file), 'r') as f:
                try:
                    yield json.load(f)
                except:
                    with open(os.path.abspath(file), 'r') as f:
                        for line in f.readlines():
                            try:
                                yield json.loads(line)
                            except:
                                yield None

def df_rows_to_list(df:pd.DataFrame, number_of_rows:int=None):
    list_of_row_values = []
    number_of_rows = number_of_rows if number_of_rows else df.shape[0]
    for index in range(0, number_of_rows):
        list_of_row_values.append(list(df.values[index]))
    
    return list_of_row_values

def list_to_string_io(list_of_entries:list):
    """
    Return file like object of the type StringIO from a given list of list of strings.
    Argument:
        - list_of_entries {list} - list of list of strings to transform to StringIO
    Example:
        [
             ['AR8IEZO1187B99055E', 'SOINLJW12A8C13314C', 'City Slickers', 2008, 149.86404],
             ['AR558FS1187FB45658', 'SOGDBUF12A8C140FAA', 'Intro', 2003, 75.67628]
        ]
        
    Return:
        {StringIO} - file type object with values in input list concatenated.
    
    Example:
        'AR8IEZO1187B99055E\tSOINLJW12A8C13314C\tCity Slickers\t2008\t149.86404\n
         AR558FS1187FB45658\tSOGDBUF12A8C140FAA\tIntro\t2003\t75.67628'
    """
    return StringIO('\n'.join(['\t'.join([str(entry) for entry in set_of_entries]) for set_of_entries in list_of_entries]))


def convert_timestamp(timestamp_miliseconds_list:list):
    series = []
    for record in timestamp_miliseconds_list:
         series.append(datetime.datetime.fromtimestamp(record/1000.0))
    return series

class Config(object):
    songs_table_name = "songs"
    artists_table_name = "artists"
    time_table_name = "time"
    users_table_name = "users"
    songsplay_table_name = 'songplays'
    table_info ={
        songs_table_name: {'artist_id': 'CHAR(18) NOT NULL',
                           'song_id': 'CHAR(18) NOT NULL',
                           'title': 'VARCHAR(100)',
                           'year': 'INTEGER',
                           'duration':'REAL'
                          },
        artists_table_name: {'artist_id': 'CHAR(18) NOT NULL',
                             'artist_name': 'VARCHAR(100)',
                             'artist_location': 'VARCHAR(100)',
                             'artist_latitude': 'REAL',
                             'artist_longitude':'REAL'
                          },
        time_table_name: {'day': 'INTEGER',
                          'hour': 'INTEGER',
                          'month': 'INTEGER',
                          'timestamp': 'TIMESTAMP(6)',
                          'week': 'INTEGER',
                          'weekday': 'INTEGER',
                          'year': 'INTEGER',       
                        },
        users_table_name: {'userId': 'INTEGER',
                          'firstName': 'VARCHAR(55)',
                          'lastName': 'VARCHAR(55)',
                          'gender': 'CHAR(1)',
                          'level': 'CHAR(10)'   
                            },
        songsplay_table_name:{
                    "ts":"BIGINT",
                     "userId":"INTEGER",
                     "level":"CHAR(10)",
                     "song_id":"CHAR(18)",
                     "artist_id":"CHAR(18)",
                     "sessionId":"INTEGER",
                     "location":"VARCHAR(255)",
                     "userAgent":"VARCHAR(255)",
            }
        }

    
def get_song_artist_id(postgre:Postgre, song_title:str, artist_name, song_duration:int, config:Config):
    results = [None, None]
    if all((song_title, artist_name, song_duration)):
        song_select ="""SELECT DISTINCT a.song_id, a.artist_id
                    FROM {SONGS_TABLE_NAME} a
                    FULL JOIN {ARTISTS_TABLE_NAME} b ON a.artist_id = b.artist_id
                    WHERE a.title='{SONG_TITLE}'
                    -- AND a.duration={SONG_DURATION}
                    -- AND b.artist_name='{ARTIST_NAME}'
                    """

        results = postgre._execute_query(song_select.format(
                                                    SONGS_TABLE_NAME=config.songs_table_name,
                                                    ARTISTS_TABLE_NAME=config.artists_table_name,
                                                    SONG_TITLE=song_title.replace("'","''"),
                                                    SONG_DURATION=song_duration,
                                                    ARTIST_NAME=artist_name.replace("'","''") ), 
                                         results=True)
        if results:
            results = list(results[0].values())

    return results    

def main():
    sparkifydb = 'sparkifydb'
    Postgre('studentdb').create_database(sparkifydb)
    postgre = Postgre(sparkifydb)
    config = Config()
    
    songs_filepath = "./data/song_data"
    songs_data =[json_data for json_data in get_json_data(songs_filepath) if json_data]
    songs_df = pd.DataFrame.from_dict(songs_data)
    
    logs_filepath = "./data/log_data"
    logs_data =[json_data for json_data in get_json_data(logs_filepath) if json_data]
    logs_df = pd.DataFrame.from_dict(logs_data)
    
    songs_column_names = list(config.table_info.get(config.songs_table_name).keys())
    song_selected_df = songs_df[songs_column_names]
    postgre.create_table(table_name=config.songs_table_name, columns_dict=config.table_info.get(config.songs_table_name))
    
    artists_column_names = list(config.table_info.get(config.artists_table_name).keys())
    artist_df = songs_df[artists_column_names]
    postgre.create_table(table_name=config.artists_table_name, columns_dict=config.table_info.get(config.artists_table_name))
    
    time_df = logs_df[logs_df['page'] == 'NextSong'].reset_index(drop=True)
    time_df = time_df.apply(lambda x: convert_timestamp(x) if x.name == 'ts' else x)
    time_data = (time_df.ts, time_df.ts.dt.hour, time_df.ts.dt.day, time_df.ts.dt.week, time_df.ts.dt.month, time_df.ts.dt.year, time_df.ts.dt.weekday)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_dict = []
    for index in range(0, len(time_data[0])):
        time_dict.append({key:value for key, value in zip(column_labels, (entry[index] for entry in time_data))})
    time_df = pd.DataFrame(time_dict)
    time_column_names = list(config.table_info.get(config.time_table_name).keys())
    postgre.create_table(table_name=config.time_table_name, columns_dict=config.table_info.get(config.time_table_name))
    
    users_column_names = list(config.table_info.get(config.users_table_name).keys())
    user_df = logs_df[users_column_names]
    postgre.create_table(table_name=config.users_table_name, columns_dict=config.table_info.get(config.users_table_name))
    
    tables_dataframes ={
        config.songs_table_name: song_selected_df,
        config.artists_table_name: artist_df,
        config.time_table_name: time_df,
        config.users_table_name: user_df
        }
    for table_name, table_df in tables_dataframes.items():
        postgre.create_table(table_name=table_name, columns_dict=config.table_info.get(table_name))
        postgre.copy_dataframe_to_table(table_df, table_name)
        
    logs_df[['song_id', 'artist_id']] = logs_df.apply(lambda row: pd.Series(get_song_artist_id(postgre, 
                                                                                               row['song'], 
                                                                                               row['artist'], 
                                                                                               row['length'], 
                                                                                               config)
                                                                           ), 
                                                      axis=1)
    songplay_column_names = list(config.table_info.get(config.songsplay_table_name).keys())
    songplay_df = logs_df[songplay_column_names]
    postgre.create_table(table_name=config.songsplay_table_name, columns_dict=config.table_info.get(config.songsplay_table_name))
    postgre.copy_dataframe_to_table(songplay_df, config.songsplay_table_name)
    
    postgre.conn.close()

if __name__ == "__main__":
    main()