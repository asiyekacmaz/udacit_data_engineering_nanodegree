# Introduction

This is a data modelling project in Postgres for Udacity Data Engineering Nanodegree Program. 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analytics team is particularly interested in understanding what songs users are listening to. 
Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, 
as well as a directory with JSON metadata on the songs in their app. 
So, I created a database schema and ETL pipeline for this analysis and used data from  Million Song Dataset in json format. 


## How to run the python scripts
To create the database tables and run the ETL pipeline, you must run the following two files in the order that they are listed below

* To create tables:
`python3 create_tables.py`

* To fill tables via ETL:
`python3 etl.py`

## Files in the repository

`test.ipynb` displays the first few rows of each table to check database.
`create_tables.py` drops and creates tables.
`etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into database tables.
`etl.py` reads and processes files from song_data and log_data and loads them into my tables.
`sql_queries.py` contains all of the sql queries, and is imported into the last three files above.
`README.md` provides discussion on your project.

## Below are the tables of the database:
**Fact Table**
Table Name:**songplays** (records in log data associated with song plays i.e. records with page NextSong)
Columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**

- Table Name: **users** (users in the app)
                user_id, first_name, last_name, gender, level
- Table Name: **songs** (songs in music database)
            song_id, title, artist_id, year, duration
- Table Name: **artists** (artists in music database)
            artist_id, name, location, latitude, longitude
- Table Name: **time** (timestamps of records in songplays broken down into specific units=
            start_time, hour, day, week, month, year, weekday


