import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, asc, length

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    create spark session
    '''    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    process song data
    - read song data from S3
    - extract columns and delete duplicate in song_id to create songs table
    - write songs table to parquet files partitioned by year and artist in S3
    - extract columns and delete duplicate in artist_id to create artists table
    - write artists table to parquet files in S3
    '''    

    song_data = os.path.join(input_data,'song_data/A/A/*/*.json')
    
    df = spark.read.json(song_data)

    windowSpec_song  = Window.partitionBy("song_id").orderBy(asc("length_title"),asc("title"))
    songs_table_prepared = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                             .withColumn("length_title",length("title")) \
                             .withColumn("rn", row_number().over(windowSpec_song))
    songs_table = songs_table_prepared.filter(songs_table_prepared.rn == 1) \
                                      .select('song_id', 'title', 'artist_id', 'year', 'duration')

    songs_table.write.partitionBy("year","artist_id").mode('overwrite').\
                      parquet(output_data + "songs/songs_table.parquet")

    windowSpec_artist  = Window.partitionBy("artist_id").orderBy(asc("artist_name"),asc("artist_name"))
    artists_table_prepared = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                               .withColumn("length_artist_name",length("artist_name")) \
                               .withColumn("rn", row_number().over(windowSpec_artist))
    artists_table = artists_table_prepared.filter(artists_table_prepared.rn == 1) \
                                          .select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'), col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude'))
    
    artists_table.write.mode('overwrite').\
                        parquet(output_data + "artists/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    '''
    process log data
    - read log data from S3
    '''    
    # get filepath to log data file
    log_data = os.path.join(input_data,'log_data')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    users_table = 
    
    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    '''
    - create spark session connecting to EMR cluster and using config data from dl.cfg file
    - process song data
    - process log data
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity-datalake-sparkify-bav/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
