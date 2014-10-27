package com.raythos.sentilexo.twitter.persistence.cql;
/**
 *
 * @author yanni
 */


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.utils.DateTimeUtils;
import com.raythos.sentilexo.utils.Helpers;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwitterQLQueries {
        static Logger log = LoggerFactory.getLogger(DateTimeUtils.class); 
   
    
    public static Insert buildInsertJsonCQL(long StatusId, String json, String keyspace){
       Insert insert;
        insert = (Insert) QueryBuilder.insertInto(Helpers.quotedString(keyspace),Helpers.quotedString("jsonlog"))
            .value(Helpers.quotedString("StatusId"), StatusId)
            .value(Helpers.quotedString("json"), json)
          .setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
        
        return insert;
    }
    
    public static Insert buildInsertCQL(TwitterQueryResultItemAvro status, String keyspace){
     
       Date createdAt = new Date();
       createdAt.setTime(status.getCreatedAt());
       Date userCreatedAt = new Date(); //= DateTimeUtils.getDateFromString(status.getUserCreatedAtAsString());
       userCreatedAt.setTime(status.getUserCreatedAt());
       Insert insert;
       Map m = status.getMentions();
       Map newMap = new HashMap();
       for(Object key: m.keySet()){
            newMap.put(key.toString(), (Long)m.get(key));
    }

       insert = (Insert) QueryBuilder.insertInto(Helpers.quotedString(keyspace), Helpers.quotedString("QueryResultItems"))
            .value("\"StatusId\"", status.getStatusId())
            .value("\"CreatedAt\"",createdAt )
            .value("\"CurrentUserRetweetId\"", status.getCurrentUserRetweetId())    
            .value("\"FavoriteCount\"", status.getFavoriteCount() )
            .value("\"favourited\"", status.getFavourited() )
            .value("\"hashtags\"", status.getHashtags() )
            .value("\"InReplyToScreenName\"", (status.getInReplyToScreenName()))
            .value("\"InReplyToStatusId\"", status.getInReplyToStatusId())
            .value("\"InReplyToUserId\"", status.getInReplyToUserId() )
            .value("\"latitude\"",status.getLatitude())
            .value("\"mentions\"", newMap) 
            .value("\"Place\"",  (status.getPlace()) )
            .value("\"PossiblySensitive\"" , status.getPossiblySensitive())
            .value("\"QueryName\""  , (status.getQueryName()))
            .value("\"Query\""  , (status.getQuery()))
            .value("\"relevantQueryTerms\"", (status.getRelevantQueryTerms()))
            .value("\"Retweet\"" , status.getRetweet())
            .value("\"RetweetCount\"",status.getRetweetCount())
            .value("\"retweetStatusId\"", status.getRetweetStatusId() )
            .value("\"Retweeted\"", status.getRetweeted())
            .value("\"RetweetedByMe\"", status.getRetweetedByMe())
            .value("\"RetweetedText\"", (status.getRetweetedText()))
            .value("\"Scopes\"",status.getScopes())
            .value("\"ScreenName\"", (status.getScreenName()))
            .value("\"Source\"", (status.getSource()))
            .value("\"text\"", (status.getText()))
            .value("\"Trucated\"", status.getTrucated())
            .value("\"urls\"", status.getUrls())
            .value("\"UserId\"",status.getUserId())
            .value("\"UserName\"",(status.getUserName()))
            .value("\"UserDescription\"", (status.getUserDescription()))
            .value("\"UserLocation\"", (status.getUserLocation()))
            .value("\"UserUrl\"",(status.getUserUrl()))
            .value("\"UserisProtected\"" , status.getUserIsProtected())
            .value("\"UserFollowersCount\"" ,status.getUserFollowersCount())
            .value("\"UserCreatedAt\"" , userCreatedAt)
            .value("\"UserFriendsCount\"" ,status.getUserFriendsCount())
            .value("\"UserListedCount\"" , status.getUserListedCount())
            .value("\"UserStatusesCount\"" , status.getUserStatusesCount())
            .value("\"UserFavouritesCount\"" , status.getUserFavoritesCount())
            .setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
     return insert;
}
 
    
 
    
} 
 