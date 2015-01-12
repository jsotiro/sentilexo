DROP KEYSPACE sentilexo;
CREATE KEYSPACE sentilexo WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE sentilexo;

CREATE TABLE hashtag_totals (
  owner text,
  query text,
  period_type int,
  time_id timestamp,
  hashtag text,
  nonreetweet_total counter,
  retweet_total counter,
  total counter,
  PRIMARY KEY ((owner, query, period_type), time_id, hashtag)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE jsonlog (
  "StatusId" bigint,
  json text,
  PRIMARY KEY ("StatusId")
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE queries (
  owner text,
  queryname text,
  active boolean,
  connectionparams map<text, text>,
  earliest timestamp,
  latest timestamp,
  maxid bigint,
  minid bigint,
  queryterms list<text>,
  totalitems int,
  PRIMARY KEY ((owner), queryname)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE query_indices (
  owner text,
  queryname text,
  id bigint,
  addedbydepl bigint,
  PRIMARY KEY ((owner, queryname), id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};


CREATE TABLE deployments (
  topology text,
  deployment_time timeuuid,
  depl_id bigint,
  PRIMARY KEY ((topology), deployment_time)
) WITH CLUSTERING ORDER BY (deployment_time DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE topologies_journal (
  topology text,
  id bigint,
  query_name text,
  owner text,
  update_time timeuuid,
  PRIMARY KEY ((topology), id, query_name, owner)
) WITH
  CLUSTERING ORDER BY (id DESC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE INDEX query_indicesDeploymentIDX ON query_indices (addedbydepl);


CREATE TABLE default_settings (
  name text,
  values list<text>,
  PRIMARY KEY (name)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE sentiment_totals (
  owner text,
  query text,
  sentiment_type text, 
  period_type int,
  time_id timestamp,
  allnoretweet_total counter,
  allretweet_total counter,
  alltotal counter,
  nononretweet_total counter,
  noretweet_total counter,
  nototal counter,
  unclearnoretweet_total counter,
  unclearretweet_total counter,
  uncleartotal counter,
  yesnonretweet_total counter,
  yesretweet_total counter,
  yestotal counter,
  PRIMARY KEY ((owner, query, sentiment_type, period_type), time_id)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};




CREATE TABLE twitter_data (
  "StatusId" bigint,
  "CreatedAt" timestamp,
  "CurrentUserRetweetId" bigint,
  "FavoriteCount" int,
  "InReplyToScreenName" text,
  "InReplyToStatusId" bigint,
  "InReplyToUserId" bigint,
  "Lang" text,
  "Place" text,
  "PossiblySensitive" boolean,
  "Query" text,
  "QueryName" text,
  "Retweet" boolean,
  "RetweetCount" int,
  "Retweeted" boolean,
  "RetweetedByMe" boolean,
  "RetweetedText" text,
  "Scopes" list<text>,
  "ScreenName" text,
  "Source" text,
  "Trucated" boolean,
  "UserCreatedAt" timestamp,
  "UserDescription" text,
  "UserFavouritesCount" int,
  "UserFollowersCount" int,
  "UserFriendsCount" int,
  "UserId" bigint,
  "UserListedCount" int,
  "UserLocation" text,
  "UserName" text,
  "UserStatusesCount" int,
  "UserUrl" text,
  "UserisProtected" boolean,
  favourited boolean,
  hashtags list<text>,
  latitude double,
  longitude double,
  mentions map<text, bigint>,
  "queryOwner" text,
  "relevantQueryTerms" list<text>,
  "retweetStatusId" bigint,
  text text,
  urls list<text>,
  PRIMARY KEY (("StatusId"))
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE INDEX twitter_dataLangIDX ON twitter_data ("Lang");

CREATE INDEX twitter_dataPlaceIDX ON twitter_data ("Place");

CREATE INDEX twitter_dataQueryNameDX ON twitter_data ("QueryName");

CREATE INDEX twitter_dataRetweetIDX ON twitter_data ("Retweet");

CREATE INDEX twitter_dataRetweetedIDX ON twitter_data ("Retweeted");

CREATE INDEX twitter_dataScreenNameIDX ON twitter_data ("ScreenName");

CREATE INDEX twitter_dataUserIdIDX ON twitter_data ("UserId");

CREATE INDEX twitter_dataQueryOwnerDX ON twitter_data ("queryOwner");   

INSERT INTO default_settings(name,values) VALUES('positivekeywords',['yes', 'goforitscotland', 'ourfuturescot', 'projectfear', 'patronisingbtlady']);
INSERT INTO default_settings(name,values) VALUES('negativekeywords',['voteno', 'naw', '%no', 'no%', 'darlingwin', 'godarling', 'better', 'letsstaytogether', 'ukok']);
INSERT INTO queries(owner,queryname, queryterms) VALUES('raythos','indyref', ['indyref','voteyes','bettertogether', 'yesperation', 'nothanks', 'scotland indep', 'scottish indep', 'scotfwd']);
