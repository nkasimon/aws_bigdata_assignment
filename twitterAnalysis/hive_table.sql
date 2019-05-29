add jar s3://elasticmapreduce/samples/hive-ads/libs/jsonserde.jar;

DROP TABLE tweets;

CREATE EXTERNAL TABLE tweets (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   favorite_count BIGINT,
   retweeted_status STRUCT<
     text:STRING,
     `user`:STRUCT<screen_name:STRING,name:STRING>,
     retweet_count:INT>,
   entities STRUCT<
     urls:ARRAY<STRUCT<expanded_url:STRING>>,
     user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
     hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   `user` STRUCT<
     screen_name:STRING,
     name:STRING,
     friends_count:INT,
     followers_count:INT,
     statuses_count:INT,
     verified:BOOLEAN,
     utc_offset:INT,
     time_zone:STRING>,
   in_reply_to_screen_name STRING
 ) 
 PARTITIONED BY (processing_date STRING)
 ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
 LOCATION 's3://simus-aws-assignment/sparkS3connection/tweetOutputDataframe';

SET hive.msck.path.validation=ignore;
MSCK REPAIR TABLE tweets;
