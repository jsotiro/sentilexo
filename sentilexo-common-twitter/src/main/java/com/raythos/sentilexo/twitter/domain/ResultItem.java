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
import java.util.HashMap;
import java.util.Map;
import twitter4j.Status;

/**
 *
 * @author yanni
 */
public class ResultItem  extends PersistedEntity {
    private Map<String,Object> fields = new HashMap();
    private boolean parseData=true;
    
    public Map<String,Object> getFields() {
        return fields;
    }
    
    public ResultItem(){
    }
    
    public ResultItem(long statusId){
        super();
        fields.put(QueryResultItemFieldNames.STATUS_ID, statusId);
    }
 
    public ResultItem(Map fields){
        super();
        this.fields = fields;
    }
    
    public ResultItem(TwitterQueryResultItemAvro avroStatus){
       super(); 
       fields = QueryResultItemMapper.getFieldsMap(avroStatus);
    }
   
     public ResultItem(String queryOwner, String queryName, String queryString, Status status){
       super(); 
       fields = QueryResultItemMapper.getFieldsMapFromStatus(queryOwner, queryName, queryString, status);
    }
    
    
    
    public static boolean exists(long statusId){
      ResultItem temp = new ResultItem();
      temp.parseData = false;
      temp.fields.put(QueryResultItemFieldNames.STATUS_ID, statusId);
      return temp.load();
    }
    
    public Object get(String fieldName){
        return fields.get(fieldName);
    }

    public void put(String fieldName, Object value){
         fields.put(fieldName,value);
    }

    
    @Override
    public void valuesFromRow(Row row) {
        if (parseData) {  
            fields.clear();
            fields.put(QueryResultItemFieldNames.STATUS_ID,row.getLong(QueryResultItemFieldNames.STATUS_ID));
            fields.put(QueryResultItemFieldNames.CREATED_AT,row.getDate(QueryResultItemFieldNames.CREATED_AT) );
            fields.put(QueryResultItemFieldNames.CURRENT_USER_RETWEET_ID, row.getLong(QueryResultItemFieldNames.CURRENT_USER_RETWEET_ID));    
            fields.put(QueryResultItemFieldNames.FAVOURITE_COUNT , row.getInt(null) );
            fields.put(QueryResultItemFieldNames.FAVOURITED, row.getBool(null) );
            fields.put(QueryResultItemFieldNames.HASHTAGS, row.getList(QueryResultItemFieldNames.HASHTAGS, String.class));
            fields.put(QueryResultItemFieldNames.IN_REPLY_TO_SCREEN_NAME, row.getString(QueryResultItemFieldNames.IN_REPLY_TO_SCREEN_NAME) );
            fields.put(QueryResultItemFieldNames.IN_REPLY_TO_STATUS_ID, row.getLong(QueryResultItemFieldNames.IN_REPLY_TO_STATUS_ID) );
            fields.put(QueryResultItemFieldNames.IN_REPLY_TO_USER_ID, row.getLong(QueryResultItemFieldNames.IN_REPLY_TO_USER_ID)  );
            fields.put(QueryResultItemFieldNames.LATITUDE, row.getDouble(QueryResultItemFieldNames.LATITUDE) );
            fields.put(QueryResultItemFieldNames.LONGITUDE, row.getDouble(QueryResultItemFieldNames.LONGITUDE) );
            fields.put(QueryResultItemFieldNames.MENTIONS, row.getMap(QueryResultItemFieldNames.MENTIONS, String.class, Long.class) ) ;
            fields.put(QueryResultItemFieldNames.LANGUAGE,  row.getString(QueryResultItemFieldNames.LANGUAGE) );
            fields.put(QueryResultItemFieldNames.PLACE,  row.getString(QueryResultItemFieldNames.PLACE) );
            fields.put(QueryResultItemFieldNames.POSSIBLY_SENSITIVE , row.getBool(QueryResultItemFieldNames.POSSIBLY_SENSITIVE));
            fields.put(QueryResultItemFieldNames.QUERY_OWNER  , row.getString(QueryResultItemFieldNames.QUERY_OWNER));            
            fields.put(QueryResultItemFieldNames.QUERY_NAME  , row.getString(QueryResultItemFieldNames.QUERY_NAME));
            fields.put(QueryResultItemFieldNames.QUERY , row.getString(QueryResultItemFieldNames.QUERY));
            fields.put(QueryResultItemFieldNames.RELEVANT_QUERY_TERMS, row.getString(QueryResultItemFieldNames.RELEVANT_QUERY_TERMS));
            fields.put(QueryResultItemFieldNames.RETWEET, row.getBool(QueryResultItemFieldNames.RETWEET));
            fields.put(QueryResultItemFieldNames.RETWEET_COUNT,row.getInt(QueryResultItemFieldNames.RETWEET));
            fields.put(QueryResultItemFieldNames.RETWEET_STATUS_ID, row.getInt(QueryResultItemFieldNames.RETWEET_STATUS_ID));
            fields.put(QueryResultItemFieldNames.RETWEETED, row.getBool(QueryResultItemFieldNames.RETWEETED));
            fields.put(QueryResultItemFieldNames.RETWEETED_BY_ME, row.getBool(QueryResultItemFieldNames.RETWEETED_BY_ME));
            fields.put(QueryResultItemFieldNames.RETWEETED_TEXT, row.getString(QueryResultItemFieldNames.RETWEETED_TEXT));
            fields.put(QueryResultItemFieldNames.SCOPES,row.getList(QueryResultItemFieldNames.SCOPES, String.class));
            fields.put(QueryResultItemFieldNames.SCREEN_NAME, row.getString(QueryResultItemFieldNames.SCREEN_NAME));
            fields.put(QueryResultItemFieldNames.SOURCE, row.getString(QueryResultItemFieldNames.SOURCE));
            fields.put(QueryResultItemFieldNames.TEXT, row.getString(QueryResultItemFieldNames.TEXT));
            fields.put(QueryResultItemFieldNames.TRUNCATED, row.getBool(QueryResultItemFieldNames.TRUNCATED));
            fields.put(QueryResultItemFieldNames.URLS, row.getList(QueryResultItemFieldNames.URLS,String.class));
            fields.put(QueryResultItemFieldNames.USER_ID,row.getLong(QueryResultItemFieldNames.USER_ID));
            fields.put(QueryResultItemFieldNames.USER_NAME,row.getString(QueryResultItemFieldNames.USER_NAME));
            fields.put(QueryResultItemFieldNames.USER_DESCRIPTION, row.getString(QueryResultItemFieldNames.USER_DESCRIPTION));
            fields.put(QueryResultItemFieldNames.USER_LOCATION, row.getString(QueryResultItemFieldNames.USER_LOCATION));
            fields.put(QueryResultItemFieldNames.USER_URL,row.getString(QueryResultItemFieldNames.USER_URL));
            fields.put(QueryResultItemFieldNames.USER_IS_PROTECTED , row.getBool(QueryResultItemFieldNames.USER_IS_PROTECTED));
            fields.put(QueryResultItemFieldNames.USER_FOLLOWERS_COUNT ,row.getInt(QueryResultItemFieldNames.USER_FOLLOWERS_COUNT));
            fields.put(QueryResultItemFieldNames.USER_CREATED_AT , row.getDate(QueryResultItemFieldNames.USER_CREATED_AT));
            fields.put(QueryResultItemFieldNames.USER_FRIENDS_COUNT,row.getInt(QueryResultItemFieldNames.USER_FRIENDS_COUNT));
            fields.put(QueryResultItemFieldNames.USER_LISTED_COUNT, row.getInt(QueryResultItemFieldNames.USER_LISTED_COUNT));
            fields.put(QueryResultItemFieldNames.USER_STATUSES_COUNT, row.getInt(QueryResultItemFieldNames.USER_STATUSES_COUNT));
            fields.put(QueryResultItemFieldNames.USER_FAVOURITES_COUNT , row.getInt(QueryResultItemFieldNames.USER_FAVOURITES_COUNT));
        }     
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
       boundStm.bind(fields.get(QueryResultItemFieldNames.STATUS_ID));  
    }

    
    
    
    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
        boundStm.bind(
                fields.get(QueryResultItemFieldNames.STATUS_ID), 
                fields.get(QueryResultItemFieldNames.CREATED_AT),
                fields.get(QueryResultItemFieldNames.CURRENT_USER_RETWEET_ID),
                fields.get(QueryResultItemFieldNames.FAVOURITE_COUNT),
                fields.get(QueryResultItemFieldNames.FAVOURITED),
                fields.get(QueryResultItemFieldNames.HASHTAGS),
                fields.get(QueryResultItemFieldNames.IN_REPLY_TO_SCREEN_NAME),
                fields.get(QueryResultItemFieldNames.IN_REPLY_TO_STATUS_ID),
                fields.get(QueryResultItemFieldNames.IN_REPLY_TO_USER_ID),
                fields.get(QueryResultItemFieldNames.LATITUDE),
                fields.get(QueryResultItemFieldNames.LONGITUDE),               
                fields.get(QueryResultItemFieldNames.MENTIONS),
                fields.get(QueryResultItemFieldNames.LANGUAGE),
                fields.get(QueryResultItemFieldNames.PLACE),
                fields.get(QueryResultItemFieldNames.POSSIBLY_SENSITIVE),
                fields.get(QueryResultItemFieldNames.QUERY_OWNER),
                fields.get(QueryResultItemFieldNames.QUERY_NAME),
                fields.get(QueryResultItemFieldNames.QUERY),
                fields.get(QueryResultItemFieldNames.RELEVANT_QUERY_TERMS),
                fields.get(QueryResultItemFieldNames.RETWEET),
                fields.get(QueryResultItemFieldNames.RETWEET_COUNT),
                fields.get(QueryResultItemFieldNames.RETWEET_STATUS_ID),
                fields.get(QueryResultItemFieldNames.RETWEETED),
                fields.get(QueryResultItemFieldNames.RETWEETED_BY_ME),
                fields.get(QueryResultItemFieldNames.RETWEETED_TEXT),
                fields.get(QueryResultItemFieldNames.SCOPES),
                fields.get(QueryResultItemFieldNames.SCREEN_NAME),
                fields.get(QueryResultItemFieldNames.SOURCE),
                fields.get(QueryResultItemFieldNames.TEXT),
                fields.get(QueryResultItemFieldNames.TRUNCATED),
                fields.get(QueryResultItemFieldNames.URLS),
                fields.get(QueryResultItemFieldNames.USER_ID),
                fields.get(QueryResultItemFieldNames.USER_NAME),
                fields.get(QueryResultItemFieldNames.USER_DESCRIPTION),
                fields.get(QueryResultItemFieldNames.USER_LOCATION),
                fields.get(QueryResultItemFieldNames.USER_URL),
                fields.get(QueryResultItemFieldNames.USER_IS_PROTECTED ),
                fields.get(QueryResultItemFieldNames.USER_FOLLOWERS_COUNT),
                fields.get(QueryResultItemFieldNames.USER_CREATED_AT),
                fields.get(QueryResultItemFieldNames.USER_FRIENDS_COUNT),
                fields.get(QueryResultItemFieldNames.USER_LISTED_COUNT),
                fields.get(QueryResultItemFieldNames.USER_STATUSES_COUNT),
                fields.get(QueryResultItemFieldNames.USER_FAVOURITES_COUNT));
       boundStm.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();   
       }        
}
