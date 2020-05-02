import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function processes the song_data, 
    creates songs and artists tables 
    and writes the tables in s3
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    
    songs_path = os.path.join(output_data, "songs")
    songs_table = df.write.mode('overwrite').partitionBy('year','artist_id').parquet(songs_path)  

    # extract columns to create artists table
    artists_table = df.select(col('artist_name').alias('name'), col('artist_location').alias('location'),  
                        col('artist_latitude').alias('latitude'),col('artist_longitude').alias('longitude'))
     
    # write artists table to parquet files
    artists_path = os.path.join(output_data, "artists")
    artists_table = df.write.mode('overwrite').parquet(artists_path)

def process_log_data(spark, input_data, output_data):
    """
    This function processes the log_data, 
    creates users, time and songplays tables 
    and writes the tables in s3
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

   # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), 
                            col('lastName').alias('last_name'), 'gender', 'level')
    
    # write users table to parquet files
    users_path = os.path.join(output_data, "users") 
    users_table = df.write.mode('overwrite').parquet(users_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x) // 1000))

    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x) // 1000))
    df = df.withColumn("datetime", get_timestamp(df.ts))
    
    df = df.withColumn('hour', hour('datetime'))
    df = df.withColumn('day', dayofmonth('datetime'))
    df = df.withColumn('week', weekofyear('datetime'))
    df = df.withColumn('month', month('datetime'))
    df = df.withColumn('year', year('datetime'))
    df = df.withColumn('weekday', dayofweek('datetime'))
    
    # extract columns to create time table 
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, "time")
    time_table = df.write.mode('overwrite').partitionBy('year','month').parquet(time_path) 

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    song_df = song_df.withColumnRenamed('year', 'year_song')
    # extract columns from joined song and log datasets to create songplays table 
    song_log_df = df.join(song_df, (song_df.artist_name == df.artist) & (song_df.title == df.song))

    songplays_table = song_log_df.select('start_time', col('userId').alias('user_id'), 'level', 'song_id', 
                                         'artist_id',col('sessionId').alias('session_id'), 'location', 
                                         col('userAgent').alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_path = os.path.join(output_data, "songplays")
    songplays_table = song_log_df.write.mode('overwrite').partitionBy('year','month').parquet(songplays_path)  
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = 's3a://raywong-bucket-one'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
