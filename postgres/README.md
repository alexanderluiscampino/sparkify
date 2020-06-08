## Sparkify database
Holds the records for songs, artists information, uses informations, songs played information.

There are 5 main tables:
    - songs: Holds all informtion for each songs
    - artists: Holds information about the artist
    - time: Holds information about the times songs were played
    - users: Holds information about the users of sparkify app
    - songsplay: Holds information about the songs played by users
    
 Being able to relate information about users, songs played, time duration, location will provide massibe analytical advantage to
 predict users trends and deliver a more curated playlist of songs to users.
 
 This ETL simply designed. All JSON files contained the raw data are loaded into dataframes (Pandas). These are there manipulated, by selecting a 
 subset of the columns to create new dataframes or manipulating the data itself. 
 All tables are driven by config class which dictates the data types of the data in the Postgre database.
 Full dataframes are then loaded to their respective tables in Postgres, using the Postgre class and one of its methods `copy_dataframe_to_table`.
 This method is quite useful nowadays, since most analytical operations is performed in Pandas and loaded to a Postgre database