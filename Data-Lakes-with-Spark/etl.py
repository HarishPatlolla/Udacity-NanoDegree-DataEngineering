import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read('dl.cfg')


os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function extracts song_data from S3 processes 
                 it to extract 1. songs table  2. artists table
                 loads back the table as parquet files to S3
        
    Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files 
            output_data : S3 bucket where tables will be stored in parquet format 
            
            Parquet, an open source file format for Hadoop. Parquet stores nested data structures
            in a flat columnar format. Compared to a traditional approach where data is stored 
            in row-oriented approach, parquet is more efficient in terms of storage and performance
    """
    
    #get filepath to song data file
    #input_data='s3a://udacity-dend'; additional string ='/song-data/A/A/A/'

    
    #Sample file
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # read song data file and converting it to Spark DataFrame
    df = spark.read.json(song_data)

    #################################### SONGS TABLE ##################################################
    # extract columns to create songs table
    songs = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    #Songs table is a dimension table. There shouldn't be any duplicates
    songs = songs.dropDuplicates(['song_id'])

    #Printing number of records in the table
    print('The number of records loaded into songs table are'+str(songs.count()))

    # write songs table to parquet files partitioned by year and artist;
    #overwriting if there is a file with same name
    songs.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    
    print("######## Loading of songs to Amazon S3 is completed ######################")
    
    #################################### ARTISTS TABLE ###################################################
    # extract columns to create artists table
    artists = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    #Artists table is a dimension table. There shouldn't be any duplicates
    artists = artists.dropDuplicates(['artist_id'])
    
    #Printing number of records in the table
    print('The number of records loaded into artists table are'+str(artists.count()))


    # write artists table to parquet files;overwriting if there is a file with same name
    artists.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')

    print("######## Loading of artists to Amazon S3 is completed ######################")
    
    
    
    
    
    
def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 processes it to extract the 
                                               1. songplays
                                               2. Users
                                               3. Time
                                               
                    and then again loaded back to S3.
                    
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    #################################### USERS TABLE ###################################################
    
    
    # extract columns for users table    
    users = df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    #Users table is a dimension table. There shouldn't be any duplicates
    users = users.dropDuplicates(['userId'])
    
    #Printing number of records in the table
    print('The number of records loaded into users table are'+str(users.count()))
    
    # write users table to parquet files; overwriting if there is a file with same name
    users.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    
    print("######## Loading of users to Amazon S3 is completed ######################")

    
    ################################### TIME TABLE #################################################
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    
    time = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year') 
   )
    
    #time table is a dimension table. There shouldn't be any duplicates
    time = time.dropDuplicates(['start_time'])
    
     
    #Printing number of records in the table
    print('The number of records loaded into time table are'+str(time.count()))
   
    
    # write time table to parquet files partitioned by year and month
    time.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')
 
    print("/n ######## Loading of time table to Amazon S3 is completed ######################")
    
    
    #################################### SONGS PLAYS TABLE #################################################
    
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, song_df.title == df.song)
    
    songplays = df.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')
    )
    
    #creating a song play id
    songplays=songplays.withColumn('songplay_id',monotonically_increasing_id())

    
    #Printing number of records in the table
    print('The number of records loaded into songplays table are'+str(songplays.count()))
    
    
    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
   
    print("####################### Loading of songplays to Amazon S3 completed ######################################")


def main():
    """
    Extract songs and events data from S3, 
    Transform it into dimensional tables
    Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://spark-s3-harish/"
    
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()