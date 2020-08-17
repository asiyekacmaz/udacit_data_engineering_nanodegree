# Sparkify's Data Lake ELT process

## Summary
 
## Introduction

In this project, we will create a data lake for a music streamin startup, Sparkify. We will create ETL pipeline that extracts their data from S3, 
processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Data sources

There are two s3 data sources namely:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

 ## Parquet data schema
 
 After reading from these two data sources, we will transform it to the schema described below:
 
 #### Song Plays table

## Fact Table
## songplays :records in log data associated with song plays i.e. records with page NextSong
    songplay_id, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent

## Dimension Tables
## users :users in the app
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
## songs :songs in music database
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
## artists : artists in music database
    artist_id, 
    name, 
    location, 
    lattitude, 
    longitude
## time : timestamps of records in songplays broken down into specific units
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
Project Template