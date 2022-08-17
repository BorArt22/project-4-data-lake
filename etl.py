import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc, asc, length, lower, trim

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
    - write songs table to parquet files partitioned by year and artist_id in S3
    - extract columns and delete duplicate in artist_id to create artists table
    - write artists table to parquet files in S3
    '''    
    # read song data from S3
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    
    df = spark.read.json(song_data)
    # extract columns and delete duplicate in song_id to create songs table
    windowSpec_song  = Window.partitionBy("song_id").orderBy(asc("length_title"),asc("title"))
    songs_table_prepared = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                             .withColumn("length_title",length("title")) \
                             .withColumn("rn", row_number().over(windowSpec_song))
    songs_table = songs_table_prepared.filter(songs_table_prepared.rn == 1) \
                                      .select('song_id', 'title', 'artist_id', 'year', 'duration')
    # write songs table to parquet files partitioned by year and artist_id in S3
    songs_table.write.partitionBy("year","artist_id").mode('overwrite') \
                     .parquet(output_data + "songs/songs_table.parquet")
    # extract columns and delete duplicate in artist_id to create artists table
    windowSpec_artist  = Window.partitionBy("artist_id").orderBy(asc("artist_name"),asc("artist_name"))
    artists_table_prepared = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude') \
                               .withColumn("length_artist_name",length("artist_name")) \
                               .withColumn("rn", row_number().over(windowSpec_artist))
    artists_table = artists_table_prepared.filter(artists_table_prepared.rn == 1) \
                                          .select('artist_id', col('artist_name').alias('name'), col('artist_location').alias('location'), col('artist_latitude').alias('latitude'), col('artist_longitude').alias('longitude'))
    # write artists table to parquet files in S3
    artists_table.write.mode('overwrite') \
                       .parquet(output_data + "artists/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    '''
    process log data
    - read log data from S3
    - filter records only from page NextSong
    - extract columns and delete duplicate in user_id to create users table
    - write users table to parquet files in S3
    - extract columns and delete duplicate in start_time to create time table
    - write time table to parquet files partitioned by year and month in S3
    - extract columns from joined song and log datasets to create songplays table 
    - write songplays table to parquet files partitioned by year and month in S3
    '''    
    
    # read log data from S3
    log_data = os.path.join(input_data,'log_data/*/*/*.json')
    df = spark.read.json(log_data)
    # filter records only from page NextSong
    df = df.filter(df.page == 'NextSong')
    # extract columns and delete duplicate in user_id to create users table
    windowSpec_users  = Window.partitionBy("userId").orderBy(desc("ts"))
    users_table_prepared = df.select('userId', 'firstName', 'lastName', 'gender', 'level', 'ts') \
                             .withColumn("rn", row_number().over(windowSpec_users))
    users_table = users_table_prepared.filter(users_table_prepared.rn == 1) \
                                      .select(col('userId').alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name'), 'gender', 'level')
    # write users table to parquet files in S3
    users_table.write.mode('overwrite') \
                     .parquet(output_data + "users/users_table.parquet")
    # extract columns and delete duplicate in start_time to create time table
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0)strftime("%m-%d-%Y %H:%M:%S.%f")[:-3])
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("ts_timestamp",get_timestamp("ts")) \
           .withColumn("ts_datetime",get_datetime("ts"))
    
    time_table = df.select(col('ts_timestamp').alias('start_time')).dropDuplicates() \
                   .withColumn("hour",hour("start_time")) \
                   .withColumn("day",dayofmonth("start_time")) \
                   .withColumn("week",weekofyear("start_time")) \
                   .withColumn("month",month("start_time")) \
                   .withColumn("year",year("start_time")) \
                   .withColumn("weekday",date_format(col("start_time"), "u"))
    
    # write time table to parquet files partitioned by year and month in S3
    time_table.write.partitionBy("year","month").mode('overwrite') \
              .parquet(output_data + "time/time_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)
    windowSpec_songplay  = Window.orderBy(asc("start_time"))

    songplays_table = df.join(song_df, \
                            (lower(trim(song_df.artist_name)) == lower(trim(df.artist))) & (lower(trim(song_df.title)) == lower(trim(df.song))), \
                            "left") \
                        .select(col('ts_timestamp').alias('start_time'), col('userId').alias('user_id'), \
                                'level','song_id','artist_id', \
                                col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent')) \
                        .withColumn("songplay_id",row_number().over(windowSpec_songplay)) \
                        .withColumn("year",year("start_time")) \
                        .withColumn("month",month("start_time"))

    # write songplays table to parquet files partitioned by year and month in S3
    songplays_table.write.partitionBy("year","month").mode('overwrite') \
                   .parquet(output_data + "songplays/songplays_table.parquet")


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
