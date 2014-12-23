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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.raythos.sentilexo.common.utils.DateTimeUtils;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class DataManager {
    
    static final Logger log = LoggerFactory.getLogger(DataManager.class);
     
    private static DataManager instance; 
    private String host;
    private String keyspace;
    private Cluster cluster;
    private Session session;
    
    private EntityPersister queryPersister;    
    private EntityPersister queryIndexPersister;
    private EntityPersister queryResultItemPersister;
    private EntityPersister resultJsonLogPersister; 
    private EntityPersister settingsPersister;
    
    private EntityPersister hashtagTotalsPersister;
    private EntityPersister sentimentTotalsItemPersister;
    
    private Map persistersForClassNames = new HashMap();
    
    public EntityPersister getPersisterFor(PersistedEntity entity){
       return (EntityPersister)persistersForClassNames.get(entity.getClass().getCanonicalName());
    }
    
   
        
   public static DataManager getInstance(){
      if (instance==null){
          instance = new DataManager();
          DateTimeUtils.initTimezonesToUTC();
      } 
      return instance;
   }   
  
 
  void traceExecutionResult(String info, ResultSet results){
      if (results.one()!=null)
                log.trace(info +" execution returned " + results.one().toString());
  }
      
   public long loadLastDeployment() {
     return 0L;  
   }
  
   public void updateDeployment(long DeploymentNo, Date deployedWhen){
           
   }
  
  
  
    void setupPersisters(){
        queryPersister = new EntityPersister();
        queryPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.loadQuery);
        queryPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.saveQuery);
        queryPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.TwitterQuery.class.getCanonicalName(),queryPersister);

        queryResultItemPersister = new EntityPersister();
        queryResultItemPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectStatus);
        queryResultItemPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertStatus);
        queryResultItemPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.TwitterResultItem.class.getCanonicalName(),queryResultItemPersister);

        
        queryIndexPersister = new EntityPersister();
        queryIndexPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectQueryIndex);
        queryIndexPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertQueryResultIndex);
        queryIndexPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.TwitterQueryIndex.class.getCanonicalName(),queryIndexPersister);

        resultJsonLogPersister = new EntityPersister();
        resultJsonLogPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectResultItemJson);
        resultJsonLogPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertResultItemJson);
        resultJsonLogPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.TwitterResultJson.class.getCanonicalName(), resultJsonLogPersister);
        
        hashtagTotalsPersister = new EntityPersister();
        sentimentTotalsItemPersister = new EntityPersister();
        
 
        
    }
  
  public Session connect(String host, String keyspace){
      this.host = host;
      this.keyspace = keyspace;
      cluster = Cluster.builder().addContactPoint(host).build();
      session = cluster.connect(keyspace); 
      RegisteredPreparedStatements.prepareSentimentStatements(session);
      setupPersisters();
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
      BoundStatement boundSelectSettingsStatement = new BoundStatement(RegisteredPreparedStatements.selectKeywords);
      boundSelectSettingsStatement.bind(name);
      ResultSet results = session.execute(boundSelectSettingsStatement);
      keywords = results.all().get(0).getList(0, String.class);
      return keywords;
   } 
  
   
   
   
   public void  updateSentimentTotalsCQL(String method, String owner, String queryName, String sentiment, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement  boundUpdateSentimentTotals=null; 
        PreparedStatement stmt = null;
        switch (sentiment) {
             case "YES":
                   if (retweet==0) stmt = RegisteredPreparedStatements.updateSentimentTotalsYesRetweets;
                   else stmt = RegisteredPreparedStatements.updateSentimentTotalsYesNoRetweets ;        
                 break;
             case "NO":
                     if (retweet==0) stmt =RegisteredPreparedStatements.updateSentimentTotalsNoRetweets;
                     else stmt =RegisteredPreparedStatements.updateSentimentTotalsNoNoRetweets;        
                 break;
             default:
                 if (retweet==0) stmt =RegisteredPreparedStatements.updateSentimentTotalsUnlcearRetweets;
                 else stmt =RegisteredPreparedStatements.updateSentimentTotalsUnlcearNoRetweets;   
             break;
         } 
        boundUpdateSentimentTotals = new BoundStatement(stmt);
        boundUpdateSentimentTotals.bind(1L,1L,1L,1L,method, owner, queryName,period_type,time_id);
        session.execute(boundUpdateSentimentTotals);
        
}
   
   public void updateSentimentTotals(String method, String owner, String query, String sentiment, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateSentimentTotalsCQL(method,owner, query,sentiment, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
    }
   
    public void  updateHashtatgTotalsCQL(String owner,String queryName, String hashtag, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement boundUpdateTotalStatement = new BoundStatement(RegisteredPreparedStatements.updateHashTagTotals);
        BoundStatement boundUpdateTotalByRetweetStatement = null;
        
        if (retweet == 0)
            boundUpdateTotalByRetweetStatement = new BoundStatement(RegisteredPreparedStatements.updateHashTagNonRetweetTotals);
        else
            boundUpdateTotalByRetweetStatement =  new BoundStatement(RegisteredPreparedStatements.updateHashTagRetweetTotals);
        
        
        boundUpdateTotalStatement.bind(1L,owner, queryName,period_type,time_id,hashtag);
        boundUpdateTotalByRetweetStatement.bind(1L,owner, queryName,period_type,time_id,hashtag);
            
        session.execute(boundUpdateTotalStatement);
        session.execute(boundUpdateTotalByRetweetStatement);
        
}

   
    
    public void updateHashTagTotals(String owner, String query, String hashtag, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateHashtatgTotalsCQL(owner, query,hashtag, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
    }
   
}
