# Sparkify Song Analysis Spark ETL Pipeline
## Goals
Create an ETL pipeline that extracts data from S3, and transforms data into a set of dimensional tables, and load them into another S3 bucket for the analytics team to continue finding insights into what songs and artists their users are listening to.

* What time of day is an artist/song typically listened to?
* What are paid users listening to the most?
* What is the top listened to artist/song by location?
* What is an artist's most listened to song?
* What is an artist's total listens across all of their songs?

## Schema
![Star Schema](https://i.imgur.com/Sfoy2Op.png)

Data is dimensionalised into the users, songs, artists, and time tables and the required Fact table 'songplays' is created on which the team will run their queries. Analysis able to be done initially on the IDs in the fact table for performance benefits.

### Fact Table
    1. **songplays** - records in log data associated with song plays i.e. records with page `NextSong`
        - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables
    1. **users** - users in the app
        - *user_id, first_name, last_name, gender, level*
    2. **songs** - songs in music database
        - *song_id, title, artist_id, year, duration*
    3. **artists** - artists in music database
        - *artist_id, name, location, latitude, longitude*
    4. **time** - timestamps of records in **songplays** broken down into specific units
        - *start_time, hour, day, week, month, year, weekday*


## Pipeline Steps
Song and event log data is loaded from S3 into Spark.

Transformation is required on the data coming from the existing JSON logs in order to create the dimension and fact tables:

1. Grouping data points into dimension tables (ex. user_id with first_name)
2. Transforming the UNIX epoch timestamp into a datetime and its components (hours, months, etc) for easy access to each
3. Matching artist name and song title from the event logs to the songs to add a song_id and artist_id to the songplays table.
    
## Sample Queries
Number of listens by artist:

    SELECT a.name, COUNT(s.session_id) FROM songplays s JOIN artists a ON s.artist_id = a.artist_id GROUP BY a.name;
    
Listen counts by song by location:

    SELECT sp.location, s.title, COUNT(sp.session_id) FROM songplays sp JOIN songs s ON sp.song_id = s.song_id GROUP BY s.title, sp.location
    
Song listen counts by paid users:

    SELECT s.title, COUNT(sp.session_id) FROM songplays sp JOIN songs s ON sp.song_id = s.song_id  WHERE sp.level = 'paid' GROUP BY s.title

## Running the Project
In either AWS console or via the AWS CLI, create:
1. Create an S3 bucket for parquet file output (data lake)
2. Install Python, Java (OpenJDK8+), and Apache Spark
3. Create running Spark cluster, locally accessible (see docker-compose up to auto-create a local cluster on spark-network)
4. Run python etl.py (alternately submit to spark)
   

## Example Config File

    AWS_ACCESS_KEY_ID=''
    AWS_SECRET_ACCESS_KEY=''
    INPUT='location of song and log files'
    OUTPUT='location to write processed parquet files'