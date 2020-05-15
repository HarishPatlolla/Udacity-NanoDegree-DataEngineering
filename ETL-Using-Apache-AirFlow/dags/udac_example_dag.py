from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity', #OwnerName
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5), #Retry every 5minutes
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *', #Hourly
          catchup=False
        )

#########################################################################################
##########################BEGIN EXECUTION                    ######################
#########################################################################################


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#########################################################################################
##########################Loading the events data from S3 to redshift######################
#########################################################################################

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events', #taskid
    dag=dag,#dagName
    table="staging_events", #Table to be inserted into
    redshift_conn_id="redshift", #RedShiftConnectionName (Created Under Airflow--->Admin--->Connection)
    aws_credentials_id="aws_credentials", #AwsCredentials ( Created  Under Airflow--->Admin--->Connection)
    s3_bucket="udacity-dend", #The location of S3 bucket where data is residing
    s3_key="log_data", 
    json_path="s3://udacity-dend/log_json_path.json", #Path to the Json file
    file_type="json" #type of the file
)

#########################################################################################
##########################Loading the songs data from S3 to redshift######################
#########################################################################################

##### USING THE StageToRedshiftOperator Custom Operator##########

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs', #taskid
    dag=dag,#dagName
    table="staging_songs",#Table to be inserted into
    redshift_conn_id="redshift",#RedShiftConnectionName (Created Under Airflow--->Admin--->Connection)
    aws_credentials_id="aws_credentials", #AwsCredentials ( Created  Under Airflow--->Admin--->Connection)
    s3_bucket="udacity-dend",#The location of S3 bucket where data is residing
    s3_key="song_data/A/A/A",
    json_path="auto",#Path to the Json file
    file_type="json"#FileType
)


#########################################################################################
#Loading into songplays table in redshift by joining staging_events and staging_songs 
#             songplay_table_insert =      SELECT
#                 md5(events.sessionid || events.start_time) songplay_id,
#                 events.start_time, 
#                 events.userid, 
#                 events.level, 
#                 songs.song_id, 
#                 songs.artist_id, 
#                 events.sessionid, 
#                 events.location, 
#                 events.useragent
#                 FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
#             FROM staging_events
#             WHERE page='NextSong') events
#             LEFT JOIN staging_songs songs
#             ON events.song = songs.title
#                 AND events.artist = songs.artist_name
#                 AND events.length = songs.duration######################
#########################################################################################


##### USING THE LoadFactOperator Custom Operator##########

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table', #taskname
    dag=dag,#dagName
    table='songplays', #table to be inserted into
    redshift_conn_id="redshift", #connectionId
    load_sql_stmt=SqlQueries.songplay_table_insert #SQL LOAD statement
 )



#########################################################################################
#Loading into users table
# user_table_insert = ("""
#         SELECT distinct userid, firstname, lastname, gender, level
#         FROM staging_events
#         WHERE page='NextSong'
#     """)
#########################################################################################



##### USING THE LoadDimensionOperator Custom Operator##########


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table', #taskid
    dag=dag,#dagName
    table='users', #Table to be inserted into
    redshift_conn_id="redshift", #connectionId
    load_sql_stmt=SqlQueries.user_table_insert, #SQL LOAD Statement,
    mode='truncate'
)



#########################################################################################
#Loading into songs table
# song_table_insert = ("""
#         SELECT distinct song_id, title, artist_id, year, duration
#         FROM staging_songs
#     """)
#########################################################################################

##### USING THE LoadDimensionOperator Custom Operator##########

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert,
    mode='truncate'
)



#########################################################################################
# #Loading into artists table
#  artist_table_insert = ("""
#         SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
#         FROM staging_songs
#     """)
#########################################################################################

##### USING THE LoadDimensionOperator Custom Operator##########

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert,
    mode='truncate'
)

#########################################################################################
# #Loading into time table
#  time_table_insert = ("""
#         SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
#                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
#         FROM songplays
#     """)
#############################################################

##### USING THE LoadDimensionOperator Custom Operator##########

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert,
    mode='truncate'
)

#########################################################################################
##########################          DATA QUALITY CHECKS           ######################
#########################################################################################


##### USING THE DataQualityOperator Custom Operator##########


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks', 
    dag=dag,
    #For testing, Dictionary has been used with statement as key and expected result as values for testing
    check_stmts =  [
             {"query":"SELECT COUNT(*) from songplays" , "expected_result":"100"} , 
             {"query":"SELECT COUNT(*) from users" , "expected_result":"100"},
             {"query":"SELECT COUNT(*) from songs" , "expected_result":"20"},
             {"query":"SELECT COUNT(*) from artists" , "expected_result":"20"},
             {"query":"SELECT COUNT(*) from time" , "expected_result":"100"},
            ],
    redshift_conn_id="redshift"
)



#########################################################################################
########################## END EXECUTION                    ######################
#########################################################################################

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)




#########################################################################################
########################## TASK DEPENDENCIES                       ######################
#########################################################################################




start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator