
# Sparkify song play logs: Datalake
This document describes how to create and load data to a datalake for analysing songs and user activity on Sparkify (music streaming app).

Original datasets reside in S3 (Amazon Simple Storage Service in Amazon Web Services - AWS), in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app.

The datalake has been created in S3 (using EMR cluster in cloud-computing platform AWS) using a star schema. ETL pipeline that transfers the original  datasets into parquet files from json has been implemented using Spark. 

# Table of Contents
1. [Project Description](#project-description)
2. [Original datasets](#original-datasets)
3. [Project Files](#project-files)
4. [Project Launching](#project-launching)
5. [Process Initial Data](#process-initial-data)
6. [Schema for Song Play Datalake](#schema-for-song-play-datalake)
7. [Example queries and results for song play analysis](#example-queries-and-results-for-song-play-analysis)

# Project Description
A music streaming startup, Sparkify, has grown their user base and song database more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this way, a Datalake with tables designed to optimize queries on song play analysis has been created and the original datasets have been loaded into it.
# Original datasets
Datasets with original datasets with JSON metadata of the songs and  JSON logs of user activity on the app reside in S3. Here are the S3 links for each:

-   Song data:  `s3://udacity-dend/song_data`
-   Log data:  `s3://udacity-dend/log_data`

Log data json path:  `s3://udacity-dend/log_json_path.json`

## Song Dataset
The first dataset is a subset of real data from the  [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
## Log Dataset
The second dataset consists of log files in JSON format.

The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

```

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like:

![](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)

# Project Files
The project workspace includes four files:

 -  `test.ipynb` displays the first few rows of each table to let check database and run queries from example.
 -  `etl.py`  loads data from S3, transforms and save to S3 in parquet files.
 -  `dl.cfg-example.properties`  example of config file for running `etl.py`.
 -  `README.md`  provides discussion on the project.
 
# Running in IPython
**Load data**
Run  `etl.py`  to read and process files from  `song_data`  and  `log_data`  and write data to parquet files in S3.
```
run etl.py
```

# Process initial data
## Process song data
1. Read all json files from `s3://udacity-dend/song_data`;
2. Extract columns (*song_id, title, artist_id, year, duration*) and delete duplicate in song_id to create songs table;
3. Write songs table to parquet files partitioned by year and artist_id in `s3://udacity-datalake-sparkify-bav/songs`;
4. Extract columns (*artist_id, name, location, lattitude, longitude*) and delete duplicate in artist_id to create artists table;
5. Write artists table to parquet files in `s3://udacity-datalake-sparkify-bav/artists`.
## Process log data
1. Read all json files from `s3://udacity-dend/log_data`;
2. Filter records only from page `NextSong`;
3. Extract columns (*user_id, first_name, last_name, gender, level*) and delete duplicate in user_id to create users table;
4. Write users table to parquet files in `s3://udacity-datalake-sparkify-bav/users`;
5. Add new fields `ts_timestamp` converting field `ts` from timestamp in milliseconds to timestamp string;
6. Extract columns (_start_time_ as `ts_timestamp` ; _hour, day, week, month, year, weekday_ extract from `ts_timestamp` using pyspark functions) and delete duplicate in start_time to create time table;
7. Write time table to parquet files partitioned by year and month in `s3://udacity-datalake-sparkify-bav/time`;
8. Extract columns (*songplay_id* as row number function; *start_time* as `ts_timestamp`; *user_id, level, song_id, artist_id, session_id, location, user_agent* from joined song and log datasets) to create songplays table;
9. Write songplays table to parquet files partitioned by year and month in  `s3://udacity-datalake-sparkify-bav/songplays`.
# Schema for Song Play Datalake
##  Fact Table
### songplays
records in log data associated with song plays i.e. records with page `NextSong`
|Field|Data Type|Description|
|--|--|--|
|start_time|STRING|Start time of songplay|
|user_id|STRING|Indeficator of user|
|level|STRING|User level|
|song_id|STRING|Indeficator of song|
|artist_id|STRING|Indeficator of artist|
|session_id|LONG|Indeficator of session|
|location|STRING|Location of songplay|
|user_agent|STRING|User agent|
|songplay_id|INTEGER|Indeficator of songplay|
|year|INTEGER|Year|
|month|INTEGER|Number of month|
## Dimension Tables
### users
users in the app
|Field|Data Type|Description|
|--|--|--|
|user_id|STRING|Indeficator of user|
|first_name|STRING|First name of user|
|last_name|STRING|Last name of user|
|gender|STRING|Gender of user (F - female, M - male|
|level|STRING|User level|
### songs
songs in music database
|Field|Data Type|Description|
|--|--|--|
|song_id|STRING|Indeficator of song|
|title|STRING|Song title|
|duration|DOUBLE|Duration of song in seconds|
|year|INTEGER|Released year of song|
|artist_id|STRING|Indeficator of artist|
### artists
artists in music database
|Field|Data Type|Description|
|--|--|--|
|artist_id|STRING|Indeficator of artist|
|name|STRING|Artist name|
|location|STRING|Artist location|
|latitude|DOUBLE|Latitude of artist location|
|longitude|DOUBLE|Longitude of artist location|
### time
timestamps of records in  **songplays**  broken down into specific units
|Field|Data Type|Description|
|--|--|--|
|start_time|STRING|Start time of songplay|
|hour|INTEGER|Hour|
|day|INTEGER|Day|
|week|INTEGER|Number of week|
|weekday|STRING|Number of dayweek|
|year|INTEGER|Year|
|month|INTEGER|Number of month|
# Example queries and results for song play analysis
### Query 1: Find all the users that has paid account and listen more than 10 songs, who are they
```
df_listofusers = songplays.filter(songplays.level == 'paid') \
                          .select("user_id") \
                          .groupBy("user_id") \
                          .agg({'user_id':'count'}) \
                          .withColumnRenamed('count(user_id)', 'countforusers')
```
```
df_listofusers.sort(desc('countforusers')) \
              .filter(df_listofusers.countforusers > 10) \
              .join(users, users.user_id == df_listofusers.user_id) \
              .select(users.first_name,users.last_name) \
              .show()
```
|first_name|last_name|
|--|--|
|     Avery|  Watkins|
|    Jaleah|    Hayes|
|    Harper|  Barrett|
|     Amiya| Davidson|
|   Kinsley|    Young|
|    Hayden|    Brock|
|     Avery| Martinez|
|     Emily|   Benson|
|  Mohammad|Rodriguez|
|     Rylan|   George|
|Jacqueline|    Lynch|
|    Jayden|   Graves|
|   Matthew|    Jones|
|     Layla|  Griffin|
|     Chloe|   Cuevas|
|     Tegan|   Levine|
|    Aleena|    Kirby|
|      Kate|  Harrell|
|      Sara|  Johnson|
|     Jacob|    Klein|
### Query 2:  Most popular artist among paid users 
```
df_artist = songplays.filter(songplays.level == 'paid') \
                          .select("artist_id") \
                          .groupBy("artist_id") \
                          .agg({'artist_id':'count'}) \
                          .withColumnRenamed('count(artist_id)', 'count_artist')
```
```
df_artist.sort(desc('count_artist')) \
         .join(artists, artists.artist_id==df_artist.artist_id) \
         .select(artists.name) \
         .show(1)
```
|artist_name|
|--|
|No Mercy|
### Query 3: How much did User 2 listen songs
```
songplays.filter(songplays.user_id == 2) \
         .select("user_id") \
         .groupBy("user_id") \
         .agg({'user_id':'count'}) \
         .withColumnRenamed('count(user_id)', 'count_listening') \
         .show()
```
|user_id|count_listening|
|--|--|
|2|10|