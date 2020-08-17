### Project: Cloud Data Warehouse

#### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I will build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. To complete this project, I will create tables in Amazon Redshift and load data from S3 to Amazon Redhift database tables. 

#### How To Run:
To run this project, I have configured dwh.cfg file in root folder. 

I have created a new notebook named as "test_project.ipynb". Within this notebook, firstly, I have created a new cluster and 
added policies.

Secondly, I have executed create_tables.py in this notebook. 

Finally, I ran etl.py and executed some SQL statements to test whether whole process is succeded.

#### Schema for Song Play Analysis
Using the song and event datasets, I have created sataging tables and a star schema optimized for queries on song play analysis. This includes the following tables.

##### Staging Tables:
stg_events: includes users' songplay activities
*event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId*

stg_songs: contains metadata about a song and the artist of that song. 
*num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year*

##### Fact Table
**songplay :** records in event data associated with song plays i.e. records with page NextSong
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension Tables
**users :** users in the app
*user_id, first_name, last_name, gender, level

**songs :** songs in music database
*song_id, title, artist_id, year, duration

**artists :** artists in music database
*artist_id, name, location, lattitude, longitude

**time :** timestamps of records in songplays broken down into specific units
*start_time, hour, day, week, month, year, weekday

##### Project Structure
The project template includes five files:

**1. create_table.py:** includes fact and dimension tables creation for the star schema in Redshift.
**2. etl.py:** includes ETL pipeline from S3 into staging tables and finally target tables on Redshift.
**3. sql_queries.py:** includes SQL statements, which will be imported into the two other files above.
**4. README.md** is where you'll provide discussion on your process and decisions for this ETL pipeline.
**5. test_project.ipynb:** this file is created to test the project and includes cluster creation, create_tables.py and etl.py execution.