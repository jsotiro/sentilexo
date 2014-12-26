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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.raythos.sentilexo.common.utils.DateTimeUtils;
import java.util.Date;
import java.util.HashMap;
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
    
    private final EntityPersister queryPersister = new EntityPersister();    
    private final EntityPersister queryIndexPersister = new EntityPersister();
    private final EntityPersister queryResultItemPersister = new EntityPersister();
    private final EntityPersister resultJsonLogPersister = new EntityPersister();
    private final EntityPersister settingsPersister = new EntityPersister();
    private final EntityPersister deploymentsPersister = new EntityPersister();
    
   
    
    private final Map persistersForClassNames = new HashMap();
    
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
      
  
    void setupPersisters(){
        queryPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.loadQuery);
        queryPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.saveQuery);
        queryPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.Query.class.getCanonicalName(),queryPersister);

        queryResultItemPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectStatus);
        queryResultItemPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertStatus);
        queryResultItemPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.ResultItem.class.getCanonicalName(),queryResultItemPersister);

        
        queryIndexPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectQueryIndex);
        queryIndexPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertQueryResultIndex);
        queryIndexPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.QueryIndex.class.getCanonicalName(),queryIndexPersister);

        resultJsonLogPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectResultItemJson);
        resultJsonLogPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertResultItemJson);
        resultJsonLogPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.ResultJson.class.getCanonicalName(), resultJsonLogPersister);
        
        settingsPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectDefaultSetting);
        settingsPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertDefaultSetting);
        settingsPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.DefaultSetting.class.getCanonicalName(), settingsPersister);        
        
        
        deploymentsPersister.setLoadEntityPrepStmt(RegisteredPreparedStatements.selectLastDeployment);
        deploymentsPersister.setSaveEntityPrepStmt(RegisteredPreparedStatements.insertDeployment);
        deploymentsPersister.setSession(session);
        persistersForClassNames.put(com.raythos.sentilexo.twitter.domain.Deployment.class.getCanonicalName(), deploymentsPersister);        
      
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
   
}
