CREATE KEYSPACE twitterqueries WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE twitterqueries;

CREATE TABLE "QueryResultItems" (
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
  "relevantQueryTerms" list<text>,
  "retweetStatusId" bigint,
  text text,
  urls list<text>,
  PRIMARY KEY ("StatusId")
) ;

CREATE INDEX TwitterQueryResultsLangIDX ON "QueryResultItems" ("Lang");

CREATE INDEX TwitterQueryResultsPlaceIDX ON "QueryResultItems" ("Place");

CREATE INDEX TwitterQueryResultsQueryNameDX ON "QueryResultItems" ("QueryName");

CREATE INDEX TwitterQueryResultsRetweetIDX ON "QueryResultItems" ("Retweet");

CREATE INDEX TwitterQueryResultsRetweetedIDX ON "QueryResultItems" ("Retweeted");

CREATE INDEX TwitterQueryResultsScreenNameIDX ON "QueryResultItems" ("ScreenName");

CREATE INDEX TwitterQueryResultsUserIdIDX ON "QueryResultItems" ("UserId");

CREATE TABLE hashtag_totals (
  query text,
  period_type int,
  time_id timestamp,
  hashtag text,
  nonreetweet_total counter,
  retweet_total counter,
  total counter,
  PRIMARY KEY ((query, period_type), time_id, hashtag)
);

CREATE TABLE jsonlog (
  "StatusId" bigint,
  json text,
  PRIMARY KEY ("StatusId")
) ;

CREATE TABLE settings (
  name text,
  values list<text>,
  PRIMARY KEY (name)
) ;

CREATE TABLE simplesentiment_totals (
  query text,
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
  PRIMARY KEY ((query, period_type), time_id)
) ;