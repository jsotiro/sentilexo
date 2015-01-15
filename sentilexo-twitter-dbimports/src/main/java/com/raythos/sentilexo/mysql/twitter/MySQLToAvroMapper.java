/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.mysql.twitter;

import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

    
public class MySQLToAvroMapper {
    
   public static  Map<String, Long> mapfromCommaDelimitedString(String s){ 
      List wordsList =  listfromCommaDelimitedString(s);
      Map<String, Long> map = new HashMap<>();
      for (Object sitem : wordsList)  
          map.put((String)sitem,0L);
      return map;
   }
    
    public static  List<String> listfromCommaDelimitedString(String s){
         String[] words  = s.split(",");
         List<String> result =  Arrays.asList(words);
         return result;
         
    }
    

    public static TwitterQueryResultItemAvro mapItem( String queryName, ResultSet rs) throws SQLException{
       
       TwitterQueryResultItemAvro result = new TwitterQueryResultItemAvro();    
       if (queryName!=null)
           queryName = queryName.toLowerCase();
    
        result.setQueryName(queryName);
        result.setQuery(rs.getString("Query"));
        result.setQueryOwner("raythos");
        result.setStatusId(rs.getLong("StatusId"));
        result.setText(rs.getString("text"));
        result.setCreatedAt(rs.getTimestamp("CreatedAt").getTime());
        result.setCurrentUserRetweetId(rs.getLong("CurrentUserRetweetId"));
        result.setFavoriteCount(rs.getInt("FavoriteCount"));
        result.setFavourited(rs.getBoolean("favourited"));
        result.setInReplyToScreenName(rs.getString("InReplyToScreenName"));
        result.setInReplyToStatusId(rs.getLong("InReplyToStatusId"));
        result.setInReplyToUserId(rs.getLong("InReplyToUserId"));
        result.setLang(rs.getString("Lang"));
        result.setLatitude(rs.getDouble("latitude"));
        result.setLongitude(rs.getDouble("longitude"));
        result.setPlace(rs.getString("Place"));
        result.setPossiblySensitive(rs.getBoolean("PossiblySensitive"));
        result.setRetweet(rs.getBoolean("Retweet"));
        result.setRetweetCount(rs.getInt("RetweetCount"));
        result.setRetweetStatusId(rs.getLong("retweetStatusId"));
        result.setRetweeted(rs.getBoolean("Retweeted"));
        result.setRetweetedByMe(rs.getBoolean("RetweetedByMe"));
        result.setRetweetedText(rs.getString("RetweetedText"));
        result.setScreenName(rs.getString("ScreenName"));
        result.setSource(rs.getString("Source"));
        result.setTrucated(rs.getBoolean("Trucated"));
        result.setUserId(rs.getLong("UserId"));
        result.setUserLocation(rs.getString("UserLocation"));
        result.setUserName(rs.getString("UserName"));
        result.setUserDescription(rs.getString("UserDescription"));
        result.setUserUrl(rs.getString("UserUrl"));
        result.setUserIsProtected(rs.getBoolean("UserisProtected"));
        result.setUserFollowersCount(rs.getInt("UserFollowersCount"));
        result.setUserCreatedAt(rs.getTimestamp("UserCreatedAt").getTime());
        result.setUserFriendsCount(rs.getInt("UserFriendsCount"));
        result.setUserListedCount(rs.getInt("UserListedCount"));
        result.setUserStatusesCount(rs.getInt("UserStatusesCount"));
        result.setUserFavoritesCount(rs.getInt("UserFavoritesCount"));
        result.setRelevantQueryTerms(listfromCommaDelimitedString(rs.getString("relevantQueryTerms")));
        result.setHashtags(listfromCommaDelimitedString(rs.getString("hashtags")));
        result.setUrls(listfromCommaDelimitedString(rs.getString("urls")));
        result.setMentions(mapfromCommaDelimitedString(rs.getString("mentions")));
   
    
    return result;
    }

}
