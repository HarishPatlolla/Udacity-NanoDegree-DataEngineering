The objective of this project was to create a Postgres database with tables designed to optimize queries 
on song play analysis.


![alt text](https://github.com/[HarishPatlolla]/[Udacity-NanoDegree-DataEngineering]/[Data-Modeling-with-PostgreSQL]/sparkify_erd.png
raw=true)

        The star schema was implemented to design the data base for the Sparkify
        There were 1 fact table (songplays SONGPLAY_ID PRIMARY KEY) 
                   4 dimension tables (users  USER_ID PRIMARY KEY, 
                                       songs SONG_ID PRIMARY KEY, 
                                       artists ARTIST_ID PRIMARY KEY, 
                                       time START_TIME PRIMARY KEY)


Step1 : Created the DROP , CREATE and INSERT queries by choosing the valid data type and Constraints (sqlqueries.py)

Step2 : Create a script to run the DROP, CREATE and INSERT queries (createtables.py)

Step 3: Used the interactive jupyter notebook to create the dataframes and insert the dataframe data into tables (etl.ipynb)

Step 4: Create a script using the commands from the Step 3 for automation (etl.py)

Step 5: Ran the test.ipynb to check the results
