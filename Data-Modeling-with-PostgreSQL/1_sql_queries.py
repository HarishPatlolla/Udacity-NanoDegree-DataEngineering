
#SERIAL is the same as integer except that PostgreSQL will automatically generate and
#populate values into the SERIAL column. This is similar to AUTO_INCREMENT column in MySQL 
#or AUTOINCREMENT column in SQLite

#To store the whole numbers in PostgreSQL,
#you use one of the following integer types: SMALLINT , INTEGER , and BIGINT

#PRIMARY KEY IS A NOTNULL UNIQUE CONSTRAINT


# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#songplays TABLE
#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS SONGPLAYS 
                                        (songplay_id SERIAL PRIMARY KEY , 
                                         start_time VARCHAR,
                                         user_id VARCHAR, 
                                         level VARCHAR, 
                                         song_id VARCHAR,
                                         artist_id VARCHAR,
                                         session_id INT,
                                         location VARCHAR,
                                         user_agent VARCHAR);
                        """)

#user_id, first_name, last_name, gender, level
user_table_create = ("""
                CREATE TABLE IF NOT EXISTS USERS 
                    (user_id VARCHAR NOT NULL PRIMARY KEY,
                     first_name VARCHAR,
                     last_name VARCHAR, 
                     gender VARCHAR,
                    level VARCHAR);
                """)

#songs - songs in music database
#song_id, title, artist_id, year, duration
song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs
                            (song_id VARCHAR PRIMARY KEY,
                             title VARCHAR,
                             artist_id VARCHAR, 
                             year INT,
                             duration FLOAT); 
                    """)

#artists - artists in music database
#artist_id, name, location, latitude, longitude

artist_table_create = ("""
                    CREATE TABLE IF NOT EXISTS artists 
                        (artist_id VARCHAR PRIMARY KEY,
                         name VARCHAR,
                         location VARCHAR,
                         latitude FLOAT,
                         longitude FLOAT);
                     """)

#time - timestamps of records in songplays broken down into specific units
#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
                    CREATE TABLE IF NOT EXISTS time 
                        (start_time BIGINT PRIMARY KEY,
                          hour INT,
                          day INT, 
                          week INT,
                          month INT,
                          year INT,
                          weekday INT); 
                    """)

# # INSERT RECORDS

#ON CONFLICT CLAUSE

("""The optional ON CONFLICT clause specifies an alternative action to raising a unique violation or exclusion constraint violation error. For each individual row proposed for insertion, either the insertion proceeds, or, if an arbiter constraint or index specified by conflict_target is violated, the alternative conflict_action is taken. ON CONFLICT DO NOTHING simply avoids inserting a row as its alternative action. ON CONFLICT DO UPDATE updates the existing row that conflicts with the row proposed for insertion as its alternative action """)

#Using DEFAULT here generates the sequence id automaticaly as SERIAL type is defined for songplayid

songplay_table_insert = ("""
 INSERT INTO songplays(songplay_id,start_time,user_id, level, song_id, artist_id,session_id,location,user_agent)
             VALUES(DEFAULT, %s, %s, %s, %s, %s, %s ,%s ,%s)
                        """)

user_table_insert = ("""INSERT INTO users (user_id,  first_name, last_name, gender, level) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                    """)


song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                        """)

artist_table_insert =("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING;
                                """)


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                               VALUES (%s, %s, %s, %s, %s, %s, %s) 
                               ON CONFLICT DO NOTHING;
                               """)

# # FIND SONGS

song_select = ("""SELECT S.song_id, A.artist_id \
                FROM songs S JOIN  artists A \
                ON S.artist_id = S.artist_id \
                WHERE S.title = %s AND a.name = %s AND s.duration = %s;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
