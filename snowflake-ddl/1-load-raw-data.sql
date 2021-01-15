*-----------------------*/
/* CREATE INITIAL SNOWFLAKE OBJECTS */
/*-----------------------*/
create database social_media_db 
  comment = 'This is a sample database for loading data from social media platforms';

create schema stg;
create schema dm;
create schema rpt;

create or replace file format tweepy_json_format
  type = 'json';

create or replace stage twitter_stage
  file_format = tweepy_json_format;

create warehouse jobrunner_wh
    with 
    warehouse_size = 'SMALL'
    warehouse_type = 'STANDARD'
    auto_suspend = 600
    auto_resume = TRUE
    min_cluster_count = 1
    max_cluster_count = 2
    scaling_policy = 'STANDARD';
    
/*-----------------------*/
/* CREATE AND LOAD STAGING TABLE TO LOAD FROM INTERNAL STAGE */
/*-----------------------*/
create or replace table stg.stg_raw_twitter (
  filename varchar
  ,file_row_number varchar
  ,tweet_id integer
  ,tweet_json variant
  ,ins_time timestamp
  );

-- A stream is used to track changes on the staging table
create or replace stream stg_raw_twitter_stream 
  on table stg.stg_raw_twitter;

-- Use COPY INTO command to load data from internal stage into the staging table
copy into stg.stg_raw_twitter (
  filename
  ,file_row_number
  ,tweet_id
  ,tweet_json
  ,ins_time
  )
from (
  select 
   metadata$filename
  ,metadata$file_row_number
  ,parse_json($1):id 
  ,$1
  ,current_timestamp()
  from @twitter_stage
    (pattern=>'.*rawtweets_.*json.gz') t
    );

-- Look at new data in table stream
select * from stg_raw_twitter_stream;

-- Use a task to load data into the staging table
create or replace task load_stg_tweets
  warehouse = jobrunner_wh
  schedule = '15 minute'
  timezone = 'UTC'
as
  copy into stg.stg_raw_twitter (
    filename
    ,file_row_number
    ,tweet_id
    ,tweet_json
    ,ins_time
    )
  from (
    select 
    metadata$filename
    ,metadata$file_row_number
    ,parse_json($1):id 
    ,$1
    ,current_timestamp()
    from @twitter_stage
      (pattern=>'.*rawtweets_.*json.gz') t
      );