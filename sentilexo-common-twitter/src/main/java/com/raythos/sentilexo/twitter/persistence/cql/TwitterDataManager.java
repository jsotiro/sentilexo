/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.twitter.persistence.cql;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.utils.DateTimeUtils;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class TwitterDataManager {
     static final Logger log = LoggerFactory.getLogger(TwitterDataManager.class);
     
    private static TwitterDataManager instance; 
    private String host;
    private String keyspace;
    private Cluster cluster;
    private Session session;
    private PreparedStatement selectKeywords;    
    
    private PreparedStatement updateHashTagTotals;
    private PreparedStatement updateHashTagRetweetTotals;
    private PreparedStatement updateHashTagNonRetweetTotals;
   
    private PreparedStatement updateSentimentTotalsYesRetweets;
    private PreparedStatement updateSentimentTotalsYesNoRetweets;
    private PreparedStatement updateSentimentTotalsNoRetweets;
    private PreparedStatement updateSentimentTotalsNoNoRetweets;
    private PreparedStatement updateSentimentTotalsUnlcearRetweets;
    private PreparedStatement updateSentimentTotalsUnlcearNoRetweets;
    

    
    
        
   public static TwitterDataManager getInstance(){
      if (instance==null){
          instance = new TwitterDataManager();
          DateTimeUtils.initTimezonesToUTC();
      } 
      return instance;
   }   
   
   
   
   
  void traceExecutionResult(String info, ResultSet results){
      if (results.one()!=null)
                log.trace(info +" execution returned " + results.one().toString());
  }
   
  public void saveTwitterQueryResultItem( TwitterQueryResultItemAvro result){
        Insert statusInsert = TwitterQLQueries.buildInsertCQL(result,keyspace);
        try {
            ResultSet results  = session.execute(statusInsert);   
            traceExecutionResult("query result insert",results);
        }
        catch (Exception e) {
           log.error("error when executing the CQL query "+statusInsert.getQueryString() +" .Error msg"+e);
        }
   }
   
  public void saveTwitterJson(long  statusId, String json){
        Insert statusInsert = TwitterQLQueries.buildInsertJsonCQL(statusId, json, keyspace);
        try {
            ResultSet results  = session.execute(statusInsert);    
            traceExecutionResult("json insert",results);
             }
        catch (Exception e) {
           log.error("error when executing the CQL query "+statusInsert.getQueryString() +" .Error msg"+e);
        }
   }

  

  
   
   
  void prepareSimpleSentimentStatements(){
      //YES and  RETWEET
        updateSentimentTotalsYesRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?, yestotal=yestotal+?, allretweet_total=allretweet_total+?, yesretweet_total=yesretweet_total+? where query= ? and  period_type =? and time_id=?");
        //YES and NO RETWEET
        updateSentimentTotalsYesNoRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?, yestotal=yestotal+?, allnoretweet_total=allnoretweet_total+?, yesnonretweet_total= yesnonretweet_total+?  where query= ? and  period_type =? and time_id=?");
        //NO and  RETWEET
        updateSentimentTotalsNoRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?, nototal=nototal+?,  allretweet_total=allretweet_total+?, noretweet_total=noretweet_total+? where query= ? and  period_type =? and time_id=?");
        //NO and NO RETWEET
        updateSentimentTotalsNoNoRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?  , nototal=nototal+?,  allnoretweet_total=allnoretweet_total+?, nononretweet_total= nononretweet_total+? where query= ? and  period_type =? and time_id=?");
        //UNCLEAR and  RETWEET
        updateSentimentTotalsUnlcearRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?  , uncleartotal=uncleartotal+?,  allretweet_total=allretweet_total+?, unclearretweet_total=unclearretweet_total+? where query= ? and  period_type =? and time_id=?");
        //UNCLEAR and NO RETWEET
        updateSentimentTotalsUnlcearNoRetweets = session.prepare("UPDATE simplesentiment_totals SET alltotal = alltotal +?  , uncleartotal=uncleartotal+?,  allnoretweet_total=allnoretweet_total+?, unclearnoretweet_total= unclearnoretweet_total+?  where query= ? and  period_type =? and time_id=?");      
  } 
   
  public  Session connect(String host, String keyspace){
      this.host = host;
      this.keyspace = keyspace;
      cluster = Cluster.builder().addContactPoint(host).build();
      session = cluster.connect(keyspace); 
      selectKeywords = session.prepare("select values from settings where name= ?");    
      updateHashTagTotals = session.prepare("UPDATE hashtag_totals SET total = total + ? where query= ? and period_type =? and time_id=?  and hashtag=?  ;");
      updateHashTagRetweetTotals = session.prepare("UPDATE hashtag_totals SET retweet_total= retweet_total+ ? where query= ? and period_type =? and time_id=? and hashtag=?  ;");   
      updateHashTagNonRetweetTotals = session.prepare("UPDATE hashtag_totals SET nonreetweet_total= nonreetweet_total+ ? where query= ? and period_type =? and time_id=? and hashtag=?  ;");
      prepareSimpleSentimentStatements();
      return session;
   }

    public String getHost() {
        return host;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Session getSession() {
        return session;
    }

    
   public List<String> getKeywordsList(String name){
      List<String> keywords;
      BoundStatement boundSelectSettingsStatement = new BoundStatement(selectKeywords);
      boundSelectSettingsStatement.bind(name);
      ResultSet results = session.execute(boundSelectSettingsStatement);
      keywords = results.all().get(0).getList(0, String.class);
      return keywords;
   } 
  
   
   public void  updateHashtatgSentimentTotalsCQL(String queryName, String sentiment, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement  boundUpdateSentimentTotals=null; 
        PreparedStatement stmt = null;
        switch (sentiment) {
             case "YES":
                   if (retweet==0) stmt = updateSentimentTotalsYesRetweets;
                   else stmt = updateSentimentTotalsYesNoRetweets ;        
                 break;
             case "NO":
                     if (retweet==0) stmt =updateSentimentTotalsNoRetweets;
                     else stmt =updateSentimentTotalsNoNoRetweets;        
                 break;
             default:
                 if (retweet==0) stmt =updateSentimentTotalsUnlcearRetweets;
                 else stmt =updateSentimentTotalsUnlcearNoRetweets;   
             break;
         } 
        boundUpdateSentimentTotals = new BoundStatement(stmt);
        boundUpdateSentimentTotals.bind(1L,1L,1L,1L,queryName,period_type,time_id);
        session.execute(boundUpdateSentimentTotals);
        
}
   
   public void updateSimpleSentimentTotals(String query, String sentiment, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateHashtatgSentimentTotalsCQL(query,sentiment, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
    }
   
   
   
   
    public void  updateHashtatgTotalsCQL(String queryName, String hashtag, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement boundUpdateTotalStatement = new BoundStatement(updateHashTagTotals);
        
        BoundStatement boundUpdateTotalByRetweetStatement = null;
                
        
        if (retweet == 0)
            boundUpdateTotalByRetweetStatement = new BoundStatement(updateHashTagNonRetweetTotals);
        else
            boundUpdateTotalByRetweetStatement =  new BoundStatement(updateHashTagRetweetTotals);
        
        
        boundUpdateTotalStatement.bind(1L,queryName,period_type,time_id,hashtag);
        boundUpdateTotalByRetweetStatement.bind(1L,queryName,period_type,time_id,hashtag);
            
        session.execute(boundUpdateTotalStatement);
        session.execute(boundUpdateTotalByRetweetStatement);
        
}

   
    
    public void updateHashTagTotals(String query, String hashtag, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateHashtatgTotalsCQL(query,hashtag, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
    }
   
   
   
   
}
