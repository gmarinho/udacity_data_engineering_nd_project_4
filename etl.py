import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear,dayofweek, date_format
import boto3
from pyspark.sql.types import StructType, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year','duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs_table.parquet',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', col("artist_name").alias('name'),col("artist_location").alias('location'), col("artist_latitude").alias('latitude'),col("artist_longitude").alias('longitude')).dropDuplicates()
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists_table.parquet',mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    """
    To get the most recent information regarding an user, based on the log data, we need to get the latest action by
    every user within the log table.
    To achieve that, a temporary view will be created, and the user information will be queried using SQL.
    """
    df.createOrReplaceTempView("staging_logs")
    # extract columns for users table    
    users_table = spark.sql("""SELECT log_max.userId as user_id, log_max.firstName as first_name,
                                            log_max.lastName as last_name, log_max.gender, log_max.level
                                            FROM staging_logs log_max JOIN
                                            (SELECT max(ts) as ts, userId FROM staging_logs
                                            GROUP BY (user_id)) log_basic
                                            ON log_max.ts = log_basic.ts AND log_max.userId = log_basic.userId
                                            """)
    
    # users_table users table to parquet files
    users_table.write.parquet(output_data+'users_table.parquet',
                              mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).replace(microsecond=0), TimestampType())
    df = df.withColumn("start_time",get_timestamp("ts"))
    # extract columns to create time table
    time_table = (df
                 .withColumn('hour', hour("start_time"))
                 .withColumn('day', dayofmonth("start_time"))
                 .withColumn('week', weekofyear("start_time"))
                 .withColumn('month', month("start_time"))
                 .withColumn('year', year("start_time"))
                 .withColumn('weekday', dayofweek("start_time"))).distinct()
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time_table.parquet',mode='overwrite',  partitionBy=['year', 'month'])

    # read in song and artist data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table.parquet')
    artist_df = spark.read.parquet(output_data+'artists_table.parquet')
    staging_songs = (song_df.join(artist_df, "artist_id", "left").select("song_id", "title", "artist_id", "name", "duration"))
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(staging_songs,[df.artist == staging_songs.name,
                                            df.song == staging_songs.title,
                                            df.length  == staging_songs.duration],
                                            'left')
    songplays_table = songplays_table.join(time_table, "start_time", "left").select("start_time",
                "user_id",
                "level",
                "song_id",
                "artist_id",
                col("sessionId").alias("session_id"),
                "location",
                col("userAgent").alias("user_agent"),
                "year",
                "month")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn("songplays_id", monotonically_increasing_id()).write.parquet(output_data+'songplays_table.parquet',
                              mode='overwrite', partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    #Comment for local test
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifygsm2711/"
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
