# Sparkify Database

Sparkify is an imaginary music streaming company. 

As an exercise at udacity course I was asked to develop ETL processes for analytics database stored in S3 buckets using Spark.

## Database schema design and ETL process
Database have 2 sources (JSON): `log_data` and `song_data` stored in S3 buckets. 

ETL is processing both source files into star schema database

Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
`songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

Dimension Tables

2. users - users in the app
`user_id`, `first_name`, `last_name`, `gender`, `level`
3. songs - songs in music database
`song_id`, `title`, `artist_id`, `year`, `duration`
4. artists - artists in music database
`artist_id`, `name`, `location`, `latitude`, `longitude`
5. time - timestamps of records in songplays broken down into specific units
`start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

## Files in repository

`etl.py` - extract, transform and load processes

`dl.cfg` - credentials and setting for AWS (not uploaded to git hub)

`data` - sample data

## How to run the python scripts

After updating your AWS credentials in `dl.cfg` run `etl.py` to populate s3 buckets.