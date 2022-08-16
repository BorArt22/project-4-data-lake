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
6. [Example queries and results for song play analysis](#example-queries-and-results-for-song-play-analysis)

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

## Process log data

# Example queries and results for song play analysis
### Query 1: Find all the users that has paid account and listen more than 10 songs, who are they


### Query 2:  Most popular artist among paid users 


### Query 3: How much did User 2 listen songs
