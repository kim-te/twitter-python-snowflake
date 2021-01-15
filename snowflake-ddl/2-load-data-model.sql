/*-----------------------*/
/* CREATE AND LOAD DATA MART (DM) TABLE */
/*-----------------------*/
create or replace table dm.dm_tweets (
  created_at_utc timestamp
  ,tweet_id integer
  ,user_screenname varchar(50)
  ,text string
  ,entities variant
  ,source string
  ,ins_time timestamp
  );

-- Use a merge statement and the previously created stream to update-insert into the DM table
merge into dm.dm_tweets as target_table
  using (
    select 
    parse_json(tweet_json):created_at as created_at_utc
    ,parse_json(tweet_json):id as tweet_id
    ,parse_json(tweet_json):user.screen_name as user_screenname
    ,parse_json(tweet_json):text as text
    ,parse_json(tweet_json):entities as entities
    ,parse_json(tweet_json):source as source
    ,current_timestamp() as ins_time
    ,metadata$action
    ,metadata$isupdate
    from stg_raw_twitter_stream
    where not (metadata$action = 'DELETE' and metadata$isupdate = TRUE))
    as source_table
    on target_table.tweet_id = source_table.tweet_id
  when matched
    and source_table.metadata$action = 'INSERT'
    and source_table.metadata$isupdate then
    update set target_table.text = source_table.text
  when matched
    and source_table.metadata$action = 'DELETE' then 
    delete
  when not matched
    and source_table.metadata$action = 'INSERT' then
  insert (
    created_at_utc
    ,tweet_id
    ,user_screenname
    ,text
    ,entities
    ,source
    ,ins_time_tz
    )
    values (
      source_table.created_at_utc
      ,source_table.tweet_id
      ,source_table.user_screenname
      ,source_table.text
      ,source_table.entities
      ,source_table.source
      ,source_table.ins_time
      );

-- Use a task to load data into the DM table from the staging table
create or replace task load_dm_tweets
  warehouse = jobrunner_wh
  timezone = 'UTC'
after load_stg_tweets
as
  merge into dm.dm_tweets as target_table
  using (
    select 
    parse_json(tweet_json):created_at as created_at_utc
    ,parse_json(tweet_json):id as tweet_id
    ,parse_json(tweet_json):user.screen_name as user_screenname
    ,parse_json(tweet_json):text as text
    ,parse_json(tweet_json):entities as entities
    ,parse_json(tweet_json):source as source
    ,current_timestamp() as ins_time
    ,metadata$action
    ,metadata$isupdate
    from stg_raw_twitter_stream
    where not (metadata$action = 'DELETE' and metadata$isupdate = TRUE))
    as source_table
    on target_table.tweet_id = source_table.tweet_id
  when matched
    and source_table.metadata$action = 'INSERT'
    and source_table.metadata$isupdate then
    update set target_table.text = source_table.text
  when matched
    and source_table.metadata$action = 'DELETE' then 
    delete
  when not matched
    and source_table.metadata$action = 'INSERT' then
  insert (
    created_at_utc
    ,tweet_id
    ,user_screenname
    ,text
    ,entities
    ,source
    ,ins_time_tz
    )
    values (
      source_table.created_at_utc
      ,source_table.tweet_id
      ,source_table.user_screenname
      ,source_table.text
      ,source_table.entities
      ,source_table.source
      ,source_table.ins_time
      );

/*-----------------------*/
/* CREATE AND LOAD REPORTING (RPT) TABLE */
/*-----------------------*/
create or replace table rpt.rpt_social (
    created_date date
    ,created_at_tz varchar(50)
    ,num_users integer
    ,num_tweets integer
    ,platform varchar(50)
    ,ins_time timestamp
    ,ins_time_tz varchar(50)
  );

-- Use a task to load data into the RPT table from the DM table. This query should evolve as more social media platforms are added in.
create or replace task load_rpt_social
  warehouse = jobrunner_wh
  timezone = 'UTC'
after load_dm_tweets
as
  create or replace table rpt.rpt_social (
    created_date date
    ,created_at_tz varchar(50)
    ,num_users integer
    ,num_tweets integer
    ,platform varchar(50)
    ,ins_time timestamp
    ,ins_time_tz varchar(50)
    ) 
    as 
    select
    DATE((convert_timezone('UTC','America/New_York', created_at_utc)), 'AUTO') as created_date
    ,'America/New_York' as created_date_tz
    ,count(distinct user_screenname) as num_users
    ,count(distinct tweet_id) as num_tweets
    ,'Twitter' as platform
    ,current_timestamp() as ins_time
    ,'UTC' as ins_time_tz
    from dm.dm_tweets
    group by
    DATE((convert_timezone('UTC','America/New_York', created_at_utc)), 'AUTO')   
    ,created_date_tz
    ,platform
    ,ins_time
    ,ins_time_tz;