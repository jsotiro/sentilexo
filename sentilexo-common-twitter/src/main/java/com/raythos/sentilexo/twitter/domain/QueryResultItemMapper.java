/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com
 *
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

import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.utils.StatusArraysHelper;
import com.raythos.sentilexo.twitter.utils.TwitterUtils;
import com.raythos.sentilexo.common.utils.DateTimeUtils;
import com.raythos.sentilexo.common.utils.Helpers;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import twitter4j.Scopes;
import twitter4j.Status;
import twitter4j.User;


/**
 *
 * @author yanni
 */
public class QueryResultItemMapper {
   
    
   
    
      public static Map getFieldsMap(TwitterQueryResultItemAvro status){
        
       Date createdAt = new Date();
       createdAt.setTime(status.getCreatedAt());
       Date userCreatedAt = new Date(); //= DateTimeUtils.getDateFromString(status.getUserCreatedAtAsString());
       userCreatedAt.setTime(status.getUserCreatedAt());
       Map m = status.getMentions();
       Map newMap = new HashMap();
       for(Object key: m.keySet()){
            newMap.put(key.toString(), (Long)m.get(key));
    }
       Map<String,Object> result = new HashMap<>();
            result.put(QueryResultItemFieldNames.STATUS_ID, status.getStatusId());
            result.put(QueryResultItemFieldNames.CREATED_AT,createdAt );
            result.put(QueryResultItemFieldNames.CURRENT_USER_RETWEET_ID, status.getCurrentUserRetweetId());    
            result.put(QueryResultItemFieldNames.FAVOURITE_COUNT , status.getFavoriteCount() );
            result.put(QueryResultItemFieldNames.FAVOURITED, status.getFavourited() );
            result.put(QueryResultItemFieldNames.HASHTAGS, Helpers.cloneList(status.getHashtags()) );
            result.put(QueryResultItemFieldNames.IN_REPLY_TO_SCREEN_NAME, (status.getInReplyToScreenName()));
            result.put(QueryResultItemFieldNames.IN_REPLY_TO_STATUS_ID, status.getInReplyToStatusId());
            result.put(QueryResultItemFieldNames.IN_REPLY_TO_USER_ID, status.getInReplyToUserId() );
            result.put(QueryResultItemFieldNames.LATITUDE,status.getLatitude());
            result.put(QueryResultItemFieldNames.LONGITUDE,status.getLongitude());
            result.put(QueryResultItemFieldNames.MENTIONS, newMap) ;
            result.put(QueryResultItemFieldNames.LANGUAGE, status.getLang());  
            result.put(QueryResultItemFieldNames.PLACE,  status.getPlace() );
            result.put(QueryResultItemFieldNames.POSSIBLY_SENSITIVE , status.getPossiblySensitive());
            result.put(QueryResultItemFieldNames.QUERY_NAME  , (status.getQueryName()));
            result.put(QueryResultItemFieldNames.QUERY , (status.getQuery()));
            result.put(QueryResultItemFieldNames.RELEVANT_QUERY_TERMS, Helpers.cloneList(status.getRelevantQueryTerms()));
            result.put(QueryResultItemFieldNames.RETWEET, status.getRetweet());
            result.put(QueryResultItemFieldNames.RETWEET_COUNT,status.getRetweetCount());
            result.put(QueryResultItemFieldNames.RETWEET_STATUS_ID, status.getRetweetStatusId() );
            result.put(QueryResultItemFieldNames.RETWEETED, status.getRetweeted());
            result.put(QueryResultItemFieldNames.RETWEETED_BY_ME, status.getRetweetedByMe());
            result.put(QueryResultItemFieldNames.RETWEETED_TEXT, (status.getRetweetedText()));
            result.put(QueryResultItemFieldNames.SCOPES,Helpers.cloneList(status.getScopes()));
            result.put(QueryResultItemFieldNames.SCREEN_NAME, (status.getScreenName()));
            result.put(QueryResultItemFieldNames.SOURCE, (status.getSource()));
            result.put(QueryResultItemFieldNames.TEXT, (status.getText()));
            result.put(QueryResultItemFieldNames.TRUNCATED, status.getTrucated());
            result.put(QueryResultItemFieldNames.URLS, Helpers.cloneList(status.getUrls()));
            result.put(QueryResultItemFieldNames.USER_ID,status.getUserId());
            result.put(QueryResultItemFieldNames.USER_NAME,(status.getUserName()));
            result.put(QueryResultItemFieldNames.USER_DESCRIPTION, (status.getUserDescription()));
            result.put(QueryResultItemFieldNames.USER_LOCATION, (status.getUserLocation()));
            result.put(QueryResultItemFieldNames.USER_URL,(status.getUserUrl()));
            result.put(QueryResultItemFieldNames.USER_IS_PROTECTED , status.getUserIsProtected());
            result.put(QueryResultItemFieldNames.USER_FOLLOWERS_COUNT ,status.getUserFollowersCount());
            result.put(QueryResultItemFieldNames.USER_CREATED_AT , userCreatedAt);
            result.put(QueryResultItemFieldNames.USER_FRIENDS_COUNT,status.getUserFriendsCount());
            result.put(QueryResultItemFieldNames.USER_LISTED_COUNT, status.getUserListedCount());
            result.put(QueryResultItemFieldNames.USER_STATUSES_COUNT, status.getUserStatusesCount());
            result.put(QueryResultItemFieldNames.USER_FAVOURITES_COUNT , status.getUserFavoritesCount());
       return result;
    }
    
    
    public static QueryResultItem getItemFromAvroObject(TwitterQueryResultItemAvro item){
        QueryResultItem result = new  QueryResultItem();
        return result;
    }
    
    public static byte[] getAvroSerializedFromStatus( String queryName, String queryString, Status status) throws IOException{
        TwitterQueryResultItemAvro item = mapItem(queryName, queryString, status);
        return getAvroSerialized(item);
    }
    
    
    public static ByteArrayOutputStream getAvroSerializedStream(TwitterQueryResultItemAvro item) throws IOException {
              ByteArrayOutputStream out = new ByteArrayOutputStream();
	      DatumWriter<TwitterQueryResultItemAvro> writer = new SpecificDatumWriter<>(TwitterQueryResultItemAvro.SCHEMA$);
	      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
	      writer.write(item, encoder);
	      encoder.flush();
	      out.close();
              return out;
    }
    
    
     public static String getAvroSerializedStreamAsJson(TwitterQueryResultItemAvro item) throws IOException {
              ByteArrayOutputStream out = new ByteArrayOutputStream();
	      DatumWriter<TwitterQueryResultItemAvro> writer = new SpecificDatumWriter<>(TwitterQueryResultItemAvro.SCHEMA$);
	      Encoder encoder = EncoderFactory.get().jsonEncoder(TwitterQueryResultItemAvro.SCHEMA$,out);
	      writer.write(item, encoder);
	      encoder.flush();
	      out.close();
              return out.toString();
    }
    
    public static byte[] getAvroSerialized(TwitterQueryResultItemAvro item) throws IOException {
               ByteArrayOutputStream out = getAvroSerializedStream(item);
                return out.toByteArray();
    }
    
   
     
    public static TwitterQueryResultItemAvro mapItem( String queryName, String queryString, Status status){
      TwitterQueryResultItemAvro result = new TwitterQueryResultItemAvro();
    
       if (queryName!=null)
           queryName = queryName.toLowerCase();
        result.setQueryName(queryName);
        result.setQuery(queryString);
        result.setStatusId(status.getId());
        result.setText(status.getText());
     
        result.setRelevantQueryTerms(TwitterUtils.relevantQueryTermsFromStatus(queryString, status));
        result.setLang(status.getLang());
      
           
        result.setCreatedAt(status.getCreatedAt().getTime());
      
        User user = status.getUser();
        result.setUserId(user.getId());
        result.setScreenName(user.getScreenName());
        result.setUserLocation(user.getLocation());
        result.setUserName(user.getName());
        result.setUserDescription(user.getDescription());
        result.setUserIsProtected(user.isProtected());
        result.setUserFollowersCount(user.getFollowersCount());
        result.setUserCreatedAt(user.getCreatedAt().getTime());
        result.setUserCreatedAtAsString(DateTimeUtils.getDateAsText(user.getCreatedAt()));
        result.setCreatedAtAsString(DateTimeUtils.getDateAsText(status.getCreatedAt()));
        result.setUserFriendsCount(user.getFriendsCount());
        result.setUserListedCount(user.getListedCount());
        result.setUserStatusesCount(user.getStatusesCount());
        result.setUserFavoritesCount(user.getFavouritesCount());
        

        result.setCurrentUserRetweetId(status.getCurrentUserRetweetId());
     
        result.setInReplyToScreenName(status.getInReplyToScreenName());
        result.setInReplyToStatusId(status.getInReplyToStatusId());
        result.setInReplyToUserId(status.getInReplyToUserId());
  
      
        if (status.getGeoLocation()!=null){
            result.setLatitude(status.getGeoLocation().getLatitude());
            result.setLongitude(status.getGeoLocation().getLongitude());
        }
        
        result.setSource(status.getSource());
        result.setTrucated(status.isTruncated());
        result.setPossiblySensitive(status.isPossiblySensitive());
   
      
   
      
        result.setRetweet(status.getRetweetedStatus()!=null);
        if (result.getRetweet()){
            result.setRetweetStatusId(status.getRetweetedStatus().getId());
        }
        result.setRetweeted(status.isRetweeted());
        result.setRetweetCount(status.getRetweetCount());
        result.setRetweetedByMe(status.isRetweetedByMe());
        if (status.getRetweetedStatus()!=null){
             result.setRetweetedText(status.getRetweetedStatus().getText());
      }
      
      result.setFavoriteCount(status.getFavoriteCount());
      result.setFavourited(status.isFavorited());
     
       if (status.getPlace()!=null){
            result.setPlace(status.getPlace().getFullName());
        }
        
        Scopes scopesObj = status.getScopes();
        if (scopesObj!=null){
            List scopes = Arrays.asList(scopesObj.getPlaceIds());
            result.setScopes(scopes);
        }

    result.setHashtags(StatusArraysHelper.getHashTagsList(status));
    
    result.setUrls(StatusArraysHelper.getUrlsList(status));
    result.setMentions(StatusArraysHelper.getUserMentionMap(status));
 
    
    return result;
    }

}   
   

    
   
      
      
  