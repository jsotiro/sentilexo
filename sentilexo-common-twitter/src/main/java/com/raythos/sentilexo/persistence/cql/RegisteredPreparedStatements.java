/*
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

package com.raythos.sentilexo.persistence.cql;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.raythos.sentilexo.common.utils.Helpers;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;

/**
 *
 * @author yanni
 */
public class RegisteredPreparedStatements {
    
    public static  PreparedStatement selectDefaultSetting;    
    public static  PreparedStatement insertDefaultSetting;
 
    public static  PreparedStatement selectLastDeployment;    
    public static  PreparedStatement insertDeployment;
    

    public static  PreparedStatement selectStatus;
    public static  PreparedStatement insertStatus;
    
    public static  PreparedStatement selectResultItemJson;
    public static  PreparedStatement insertResultItemJson;
    
    public static  PreparedStatement selectQueryIndex;    
    public static  PreparedStatement insertQueryResultIndex;
    
    
    
    public static  PreparedStatement updateHashTagTotals;
    public static  PreparedStatement updateHashTagRetweetTotals;
    public static  PreparedStatement updateHashTagNonRetweetTotals;
   
    public static  PreparedStatement updateSentimentTotalsYesRetweets;
    public static  PreparedStatement updateSentimentTotalsYesNoRetweets;
    public static  PreparedStatement updateSentimentTotalsNoRetweets;
    public static  PreparedStatement updateSentimentTotalsNoNoRetweets;
    public static  PreparedStatement updateSentimentTotalsUnlcearRetweets;
    public static  PreparedStatement updateSentimentTotalsUnlcearNoRetweets;
    public static  PreparedStatement loadQuery;
    public static  PreparedStatement saveQuery;  
   

    
    static  void prepareSentimentStatements(Session session){
      //YES and  RETWEET
        updateSentimentTotalsYesRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?, yestotal=yestotal+?, allretweet_total=allretweet_total+?, yesretweet_total=yesretweet_total+? where sentiment_type=? and owner=? and query= ? and  period_type =? and time_id=?");
        //YES and NO RETWEET
        updateSentimentTotalsYesNoRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?, yestotal=yestotal+?, allnoretweet_total=allnoretweet_total+?, yesnonretweet_total= yesnonretweet_total+?  where sentiment_type=? and owner= ? and query= ? and  period_type =? and time_id=?");
        //NO and  RETWEET
        updateSentimentTotalsNoRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?, nototal=nototal+?,  allretweet_total=allretweet_total+?, noretweet_total=noretweet_total+? where sentiment_type=? and owner=? and  query= ? and  period_type =? and time_id=?");
        //NO and NO RETWEET
        updateSentimentTotalsNoNoRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?  , nototal=nototal+?,  allnoretweet_total=allnoretweet_total+?, nononretweet_total= nononretweet_total+? where sentiment_type=? and owner= ? and  query= ? and  period_type =? and time_id=?");
        //UNCLEAR and  RETWEET
        updateSentimentTotalsUnlcearRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?  , uncleartotal=uncleartotal+?,  allretweet_total=allretweet_total+?, unclearretweet_total=unclearretweet_total+? where sentiment_type=? and owner=?  and  query= ? and  period_type =? and time_id=?");
        //UNCLEAR and NO RETWEET
        updateSentimentTotalsUnlcearNoRetweets = session.prepare("UPDATE sentiment_totals SET alltotal = alltotal +?  , uncleartotal=uncleartotal+?,  allnoretweet_total=allnoretweet_total+?, unclearnoretweet_total= unclearnoretweet_total+?  where sentiment_type=? and owner=?  and  query= ? and  period_type =? and time_id=?");
        
                
        selectDefaultSetting = session.prepare("select values from default_settings where name= ?");    
        insertDefaultSetting = session.prepare("insert into default_settings(name,values) values( ?, ?)");
       
        
        selectLastDeployment = session.prepare("select  dateOf(deployment_time) as depl_time, depl_id from deployments where topology= ? limit 1");    
        insertDeployment = session.prepare("insert INTO deployments (deployment_time, topology, depl_id) VALUES (now(), ?, ?);"); 
        
        
        selectStatus = session.prepare("select * from \"twitter_data\" where \"StatusId\"=?");  
        insertStatus = session.prepare(saveResultItemCQL());
        
        selectQueryIndex = session.prepare("select * from query_indices where owner=? and queryname=? and id=?");
        insertQueryResultIndex = session.prepare("INSERT INTO query_indices(owner,queryname, id, addedbydepl ) values(?,?, ?,?);");
        
        selectResultItemJson = session.prepare("select * from jsonlog where \"StatusId\"=?");
        insertResultItemJson = session.prepare("INSERT INTO jsonlog(\"StatusId\",json) values(?,?);");
        
        
        updateHashTagTotals = session.prepare("UPDATE hashtag_totals SET total = total + ? where owner= ? and  query= ? and period_type =? and time_id=?  and hashtag=?  ;");
        updateHashTagRetweetTotals = session.prepare("UPDATE hashtag_totals SET retweet_total= retweet_total+ ? where owner= ? and query= ? and period_type =? and time_id=? and hashtag=?  ;");   
        updateHashTagNonRetweetTotals = session.prepare("UPDATE hashtag_totals SET nonreetweet_total= nonreetweet_total+ ? where owner= ? and  query= ? and period_type =? and time_id=? and hashtag=?  ;");
        
        String loadQueryCQL = "select * from queries where owner=? and queryName=?";
        String saveQueryCQL = "INSERT INTO queries(owner,   queryname,   active, queryterms, connectionparams, totalitems,earliest, latest, minid, maxid) values (?,?,?,?,?,?,?,?,?,?)";

        loadQuery = session.prepare(loadQueryCQL);
        saveQuery = session.prepare(saveQueryCQL);
        
     
  } 

  static String saveResultItemCQL(){
      StringBuilder statusInsert =  new StringBuilder("Insert INTO twitter_data(")
            .append(Helpers.quotedString(QueryResultItemFieldNames.STATUS_ID)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.CREATED_AT)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.CURRENT_USER_RETWEET_ID)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.FAVOURITE_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.FAVOURITED)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.HASHTAGS)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.IN_REPLY_TO_SCREEN_NAME)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.IN_REPLY_TO_STATUS_ID)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.IN_REPLY_TO_USER_ID)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.LATITUDE)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.LONGITUDE)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.MENTIONS)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.PLACE)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.POSSIBLY_SENSITIVE)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.QUERY_NAME)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.QUERY)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RELEVANT_QUERY_TERMS)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEET)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEET_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEET_STATUS_ID)).append(",") 
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEETED)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEETED_BY_ME)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.RETWEETED_TEXT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.SCOPES)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.SCREEN_NAME)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.SOURCE)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.TEXT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.TRUNCATED)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.URLS)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_ID)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_NAME)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_DESCRIPTION)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_LOCATION)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_URL)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_IS_PROTECTED )).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_FOLLOWERS_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_CREATED_AT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_FRIENDS_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_LISTED_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_STATUSES_COUNT)).append(",")
            .append(Helpers.quotedString(QueryResultItemFieldNames.USER_FAVOURITES_COUNT)).append(")")
            .append(" values ( ");
             for (int i=0;i<QueryResultItemFieldNames.FiedlCount-1;i++){
                statusInsert.append("?,");
              }
            statusInsert.append("? )");
   return statusInsert.toString();
  }   
  
  
  
  
       
}
