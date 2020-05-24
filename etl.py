import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, udf, col
from pyspark.sql.functions import to_timestamp, dayofweek, year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Load and transform song and artist data into dimension tables

    Arguments:
        spark {Object} -- spark session instance
        input_data {string} -- location of JSON formatted event/log data
        output_data {string} -- location to save parquet output data
    """  
    # load song data into spark
    song_data = input_data + 'song_data'
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_table_schema = ['song_id','title','artist_id','year','duration']
    songs_table = df.dropna(how='any', subset=['song_id','artist_id']) \
                    .select(song_table_schema) \
                    .dropDuplicates()

    songs_table.write.mode('overwrite') \
               .partitionBy('year','artist_id') \
               .parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artist_table_schema = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df.dropna(how='any', subset=['song_id','artist_id']) \
                      .select(artist_table_schema) \
                      .dropDuplicates()
    
    artists_table.write.mode('overwrite') \
               .partitionBy('artist_location') \
               .parquet(output_data + 'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """Load and transform song play log data into dimension tables and event fact table.

    Arguments:
        spark {Object} -- spark session instance
        input_data {string} -- location of JSON formatted event/log data
        output_data {string} -- location to save parquet output data - must match where data is saved for process_song function
    """    
    # load log data into spark
    log_data = input_data + 'log_data'
    df = spark.read.json(log_data).filter(df.page == 'NextSong')

    # Create Users table   
    users_table_schema = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.dropna(how='any', subset=['userId','firstName']) \
                    .select(users_table_schema) \
                    .dropDuplicates()

    users_table.write.mode('overwrite') \
               .partitionBy('gender') \
               .parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000.0))

    # extract columns to create time table
    time_table = df.select('start_time') \
                   .dropDuplicates() \
                   .withColumn('hour', hour(df.start_time)) \
                   .withColumn('day', dayofmonth(df.start_time)) \
                   .withColumn('week', weekofyear(df.start_time)) \
                   .withColumn('month', month(df.start_time)) \
                   .withColumn('year', year(df.start_time)) \
                   .withColumn('weekday', dayofweek(df.start_time))

    time_table.write.mode('overwrite') \
               .partitionBy('year', 'month') \
               .parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data+'songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_df, df.song == songs_df.title, how='left') \
                        .withColumn('songplay_id', monotonically_increasing_id()) \
                        .withColumn('start_time', date_format(df.start_time)) \
                        .withColumn('month', month(df.start_time)) \
                        .withColumn('year', year(df.start_time)) \
                        .selectExpr("songplay_id", "start_time", 'year', 'month', "userId as user_id", "level", 
                                     "song_id", "artist_id", "sessionId as session_id", "location", 
                                     "userAgent as user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite') \
               .partitionBy('year', 'month') \
               .parquet(output_data + 'songplays.parquet')

def main():
    """
    Create a spark session and begin ETL process
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()

    input_data = config['INPUT']
    output_data = config['OUTPUT']

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
