import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    create new spark session with specified config
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Using spark process json song_data from s3 bucket (input_data)
    into tables (dataframes): songs and artists
    and save them to s3 bucket (output_data) as parquet files
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/' #song_data/*/*/*/*.json
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Using spark process json log_data from s3 bucket (input_data)
    into tables (dataframes): users, time and songplays
    and save them to s3 bucket (output_data) as parquet files
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-12-events.json' # 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime columns from original start_time column
    df = df.withColumn("hour", hour("start_time")) \
            .withColumn("day", dayofmonth("start_time")) \
            .withColumn("week", weekofyear("start_time")) \
            .withColumn("month", month("start_time")) \
            .withColumn("year", year("start_time")) \
            .withColumn("weekday", date_format(col("start_time"), "u"))
    
    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time/"), mode='overwrite', partitionBy=["year","month"])
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs/"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='left').select(monotonically_increasing_id().alias("songplay_id"), \
                        "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent")
    
    # add year and month for partitioning
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner") \
                        .select("songplay_id", songplays_table.start_time, "year", "month", "userId", "level", \
                                "song_id", "artist_id", "sessionId", "location", "userAgent").drop_duplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-adam-klus/"
    
    print("Processing song data started")
    process_song_data(spark, input_data, output_data)
    print("Processing song data completed")
    
    print("Processing log data started")
    process_log_data(spark, input_data, output_data)
    print("Processing log data completed")

if __name__ == "__main__":
    main()
