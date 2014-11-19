/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.twitter.common.domain;

import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.common.utils.StatusArraysHelper;
import com.raythos.sentilexo.twitter.common.utils.TwitterUtils;
import com.raythos.sentilexo.utils.DateTimeUtils;
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
public class TwitterQueryResultItemMapper {
   
    
    public static List cloneList(List l){
        ArrayList<Object> list = new ArrayList();
        if (l==null) return null; 
        for (Object o : l){
            list.add(o);
        }
        return list;
    }
    
      public static Map asFieldMap(TwitterQueryResultItemAvro status){
        
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
             result.put("StatusId", status.getStatusId());
             result.put("CreatedAt",createdAt );
             result.put("CurrentUserRetweetId", status.getCurrentUserRetweetId());    
            result.put("FavoriteCount", status.getFavoriteCount() );
            result.put("favourited", status.getFavourited() );
            result.put("hashtags", cloneList(status.getHashtags()) );
            result.put("InReplyToScreenName", (status.getInReplyToScreenName()));
            result.put("InReplyToStatusId", status.getInReplyToStatusId());
            result.put("InReplyToUserId", status.getInReplyToUserId() );
            result.put("latitude",status.getLatitude());
            result.put("mentions", newMap) ;
            result.put("Place",  (status.getPlace()) );
            result.put("PossiblySensitive" , status.getPossiblySensitive());
            result.put("QueryName"  , (status.getQueryName()));
            result.put("Query"  , (status.getQuery()));
            result.put("relevantQueryTerms", cloneList(status.getRelevantQueryTerms()));
            result.put("Retweet" , status.getRetweet());
            result.put("RetweetCount",status.getRetweetCount());
            result.put("retweetStatusId", status.getRetweetStatusId() );
            result.put("Retweeted", status.getRetweeted());
            result.put("RetweetedByMe", status.getRetweetedByMe());
            result.put("RetweetedText", (status.getRetweetedText()));
            result.put("Scopes",cloneList(status.getScopes()));
            result.put("ScreenName", (status.getScreenName()));
            result.put("Source", (status.getSource()));
            result.put("text", (status.getText()));
            result.put("Trucated", status.getTrucated());
            result.put("urls", cloneList(status.getUrls()));
            result.put("UserId",status.getUserId());
            result.put("UserName",(status.getUserName()));
            result.put("UserDescription", (status.getUserDescription()));
            result.put("UserLocation", (status.getUserLocation()));
            result.put("UserUrl",(status.getUserUrl()));
            result.put("UserisProtected" , status.getUserIsProtected());
            result.put("UserFollowersCount" ,status.getUserFollowersCount());
            result.put("UserCreatedAt" , userCreatedAt);
            result.put("UserFriendsCount" ,status.getUserFriendsCount());
            result.put("UserListedCount" , status.getUserListedCount());
            result.put("UserStatusesCount" , status.getUserStatusesCount());
            result.put("UserFavouritesCount" , status.getUserFavoritesCount());
       return result;
    }
    
    
    public static TwitterQueryResultItem getItemFromAvroObject(TwitterQueryResultItemAvro item){
        TwitterQueryResultItem result = new  TwitterQueryResultItem();
       
        
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
      
      
       // result.setJson(TwitterObjectFactory.getRawJSON(status));
      
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
    
      
     // List contrs = Arrays.asList(Long.class,status.getContributors());
     //    result.setContributors(contrs);
    
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
   

    
   
      
      
  