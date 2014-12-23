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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.Serializable;

/**
 *
 * @author yanni
 */
public class EntityPersister implements Serializable {
  
   private PreparedStatement loadEntityPrepStmt;
   private PreparedStatement saveEntityPrepStmt;  
  
  private BoundStatement boundLoadStatement;
  private BoundStatement boundSaveStatement; 
  
  private Session session;
  
  
    public PreparedStatement getLoadEntityPreparedStmt() {
        return loadEntityPrepStmt;
    }

    public PreparedStatement getSaveEntityPreparedStmt() {
        return saveEntityPrepStmt;
    }

    public void setLoadEntityPrepStmt(PreparedStatement loadEntityPrepStmt) {
        this.loadEntityPrepStmt = loadEntityPrepStmt;
    }

    public void setSaveEntityPrepStmt(PreparedStatement saveEntityPrepStmt) {
        this.saveEntityPrepStmt = saveEntityPrepStmt;
    }

    
    
    public BoundStatement getBoundLoadStatement() {
        return boundLoadStatement;
    }

    public BoundStatement getBoundSaveStatement() {
        return boundSaveStatement;
    }

    public Session getSession() {
        return session;
    }
   
   public void setSession(Session session) {
        this.session = session;
        boundLoadStatement = new BoundStatement(loadEntityPrepStmt);
        boundSaveStatement = new BoundStatement(saveEntityPrepStmt);
    }
  
   
   
   public boolean load(Persistable entity){
   boolean found;
   entity.bindCQLLoadParameters(boundLoadStatement); 
   ResultSet results = session.execute(boundLoadStatement);
   found = results.getAvailableWithoutFetching() > 0;
   if (found) {
        Row row = results.all().get(0);
        entity.valuesFromRow(row);
    }     
   return found;
   }
   
   
   public void simpleSave(Persistable entity) {   
      entity.bindCQLSaveParameters(boundSaveStatement); 
      ResultSet results = session.execute(boundSaveStatement);
     }
   
   public void save(Persistable entity) {   
      entity.bindCQLSaveParameters(boundSaveStatement); 
      ResultSet results = session.execute(boundSaveStatement);
     }   
  
}
  
    
