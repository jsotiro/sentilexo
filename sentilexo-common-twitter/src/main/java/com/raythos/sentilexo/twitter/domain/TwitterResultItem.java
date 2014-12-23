/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.raythos.sentilexo.twitter.domain;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.persistence.cql.PersistedEntity;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class TwitterResultItem  extends PersistedEntity {
    private Map<String,Object> fields;
    private boolean parseData=true;

    public Map getFields() {
        return fields;
    }
    
    public TwitterResultItem(){
    }
    
    public TwitterResultItem(TwitterQueryResultItemAvro avroStatus){
       super(); 
       fields = TwitterQueryResultItemMapper.getFieldsMap(avroStatus);
    }
    
    public static boolean exists(long statusId){
      TwitterResultItem temp = new TwitterResultItem();
      temp.parseData = false;
      temp.fields.put(StatusFieldNames.STATUS_ID, statusId);
      return temp.load();
    }
    
    @Override
    public void valuesFromRow(Row row) {
        if (parseData) {  
            fields.clear();
            fields.put(StatusFieldNames.STATUS_ID,row.getLong(StatusFieldNames.STATUS_ID));
            fields.put(StatusFieldNames.CREATED_AT,row.getDate(StatusFieldNames.CREATED_AT) );
            fields.put(StatusFieldNames.CURRENT_USER_RETWEET_ID, row.getLong(StatusFieldNames.CURRENT_USER_RETWEET_ID));    
            fields.put(StatusFieldNames.FAVOURITE_COUNT , row.getInt(null) );
            fields.put(StatusFieldNames.FAVOURITED, row.getBool(null) );
            fields.put(StatusFieldNames.HASHTAGS, row.getList(StatusFieldNames.HASHTAGS, String.class));
            fields.put(StatusFieldNames.IN_REPLY_TO_SCREEN_NAME, row.getString(StatusFieldNames.IN_REPLY_TO_SCREEN_NAME) );
            fields.put(StatusFieldNames.IN_REPLY_TO_STATUS_ID, row.getLong(StatusFieldNames.IN_REPLY_TO_STATUS_ID) );
            fields.put(StatusFieldNames.IN_REPLY_TO_USER_ID, row.getLong(StatusFieldNames.IN_REPLY_TO_USER_ID)  );
            fields.put(StatusFieldNames.LATITUDE, row.getDouble(StatusFieldNames.LATITUDE) );
            fields.put(StatusFieldNames.LONGITUDE, row.getDouble(StatusFieldNames.LONGITUDE) );
            fields.put(StatusFieldNames.MENTIONS, row.getMap(StatusFieldNames.MENTIONS, String.class, Long.class) ) ;
            fields.put(StatusFieldNames.LANGUAGE,  row.getString(StatusFieldNames.LANGUAGE) );
            fields.put(StatusFieldNames.PLACE,  row.getString(StatusFieldNames.PLACE) );
            fields.put(StatusFieldNames.POSSIBLY_SENSITIVE , row.getBool(StatusFieldNames.POSSIBLY_SENSITIVE));
            fields.put(StatusFieldNames.QUERY_NAME  , row.getString(StatusFieldNames.QUERY_NAME));
            fields.put(StatusFieldNames.QUERY , row.getString(StatusFieldNames.QUERY));
            fields.put(StatusFieldNames.RELEVANT_QUERY_TERMS, row.getString(StatusFieldNames.RELEVANT_QUERY_TERMS));
            fields.put(StatusFieldNames.RETWEET, row.getBool(StatusFieldNames.RETWEET));
            fields.put(StatusFieldNames.RETWEET_COUNT,row.getInt(StatusFieldNames.RETWEET));
            fields.put(StatusFieldNames.RETWEET_STATUS_ID, row.getInt(StatusFieldNames.RETWEET_STATUS_ID));
            fields.put(StatusFieldNames.RETWEETED, row.getBool(StatusFieldNames.RETWEETED));
            fields.put(StatusFieldNames.RETWEETED_BY_ME, row.getBool(StatusFieldNames.RETWEETED_BY_ME));
            fields.put(StatusFieldNames.RETWEETED_TEXT, row.getString(StatusFieldNames.RETWEETED_TEXT));
            fields.put(StatusFieldNames.SCOPES,row.getList(StatusFieldNames.SCOPES, String.class));
            fields.put(StatusFieldNames.SCREEN_NAME, row.getString(StatusFieldNames.SCREEN_NAME));
            fields.put(StatusFieldNames.SOURCE, row.getString(StatusFieldNames.SOURCE));
            fields.put(StatusFieldNames.TEXT, row.getString(StatusFieldNames.TEXT));
            fields.put(StatusFieldNames.TRUNCATED, row.getBool(StatusFieldNames.TRUNCATED));
            fields.put(StatusFieldNames.URLS, row.getList(StatusFieldNames.URLS,String.class));
            fields.put(StatusFieldNames.USER_ID,row.getLong(StatusFieldNames.USER_ID));
            fields.put(StatusFieldNames.USER_NAME,row.getString(StatusFieldNames.USER_NAME));
            fields.put(StatusFieldNames.USER_DESCRIPTION, row.getString(StatusFieldNames.USER_DESCRIPTION));
            fields.put(StatusFieldNames.USER_LOCATION, row.getString(StatusFieldNames.USER_LOCATION));
            fields.put(StatusFieldNames.USER_URL,row.getString(StatusFieldNames.USER_URL));
            fields.put(StatusFieldNames.USER_IS_PROTECTED , row.getBool(StatusFieldNames.USER_IS_PROTECTED));
            fields.put(StatusFieldNames.USER_FOLLOWERS_COUNT ,row.getInt(StatusFieldNames.USER_FOLLOWERS_COUNT));
            fields.put(StatusFieldNames.USER_CREATED_AT , row.getDate(StatusFieldNames.USER_CREATED_AT));
            fields.put(StatusFieldNames.USER_FRIENDS_COUNT,row.getInt(StatusFieldNames.USER_FRIENDS_COUNT));
            fields.put(StatusFieldNames.USER_LISTED_COUNT, row.getInt(StatusFieldNames.USER_LISTED_COUNT));
            fields.put(StatusFieldNames.USER_STATUSES_COUNT, row.getInt(StatusFieldNames.USER_STATUSES_COUNT));
            fields.put(StatusFieldNames.USER_FAVOURITES_COUNT , row.getInt(StatusFieldNames.USER_FAVOURITES_COUNT));
        }     
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
       boundStm.bind(fields.get(StatusFieldNames.STATUS_ID));  
    }

    
    
    
    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
        boundStm.bind(
                fields.get(StatusFieldNames.STATUS_ID), 
                fields.get(StatusFieldNames.CREATED_AT),
                fields.get(StatusFieldNames.CURRENT_USER_RETWEET_ID),
                fields.get(StatusFieldNames.FAVOURITE_COUNT),
                fields.get(StatusFieldNames.FAVOURITED),
                fields.get(StatusFieldNames.HASHTAGS),
                fields.get(StatusFieldNames.IN_REPLY_TO_SCREEN_NAME),
                fields.get(StatusFieldNames.IN_REPLY_TO_STATUS_ID),
                fields.get(StatusFieldNames.IN_REPLY_TO_USER_ID),
                fields.get(StatusFieldNames.LATITUDE),
                fields.get(StatusFieldNames.MENTIONS),
                fields.get(StatusFieldNames.PLACE),
                fields.get(StatusFieldNames.POSSIBLY_SENSITIVE),
                fields.get(StatusFieldNames.QUERY_NAME),
                fields.get(StatusFieldNames.QUERY),
                fields.get(StatusFieldNames.RELEVANT_QUERY_TERMS),
                fields.get(StatusFieldNames.RETWEET),
                fields.get(StatusFieldNames.RETWEET_COUNT),
                fields.get(StatusFieldNames.RETWEET_STATUS_ID),
                fields.get(StatusFieldNames.RETWEETED),
                fields.get(StatusFieldNames.RETWEETED_BY_ME),
                fields.get(StatusFieldNames.RETWEETED_TEXT),
                fields.get(StatusFieldNames.SCOPES),
                fields.get(StatusFieldNames.SCREEN_NAME),
                fields.get(StatusFieldNames.SOURCE),
                fields.get(StatusFieldNames.TEXT),
                fields.get(StatusFieldNames.TRUNCATED),
                fields.get(StatusFieldNames.URLS),
                fields.get(StatusFieldNames.USER_ID),
                fields.get(StatusFieldNames.USER_NAME),
                fields.get(StatusFieldNames.USER_DESCRIPTION),
                fields.get(StatusFieldNames.USER_LOCATION),
                fields.get(StatusFieldNames.USER_URL),
                fields.get(StatusFieldNames.USER_IS_PROTECTED ),
                fields.get(StatusFieldNames.USER_FOLLOWERS_COUNT),
                fields.get(StatusFieldNames.USER_CREATED_AT),
                fields.get(StatusFieldNames.USER_FRIENDS_COUNT),
                fields.get(StatusFieldNames.USER_LISTED_COUNT),
                fields.get(StatusFieldNames.USER_STATUSES_COUNT),
                fields.get(StatusFieldNames.USER_FAVOURITES_COUNT));
       boundStm.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();   
       }        
}
