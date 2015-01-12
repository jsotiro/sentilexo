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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.persistence.cql.PersistedEntity;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class DeployedTopology extends PersistedEntity{
    private String name;
    private String owner;
    private String query;
    private long id; 
    private Date processed;
    protected  static Logger log = LoggerFactory.getLogger(DeployedTopology.class);
   
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getProcessed() {
        return processed;
    }

    public void setProcessed(Date processed) {
        this.processed = processed;
    }
 
    
  public DeployedTopology(String name, String owner, String query, long id, Date processed ){
      super();
      this.name=name;
      this.owner = owner;
      this.query = query;
      this.id = id;
      if (processed!=null)
        this.processed = new Date(processed.getTime());
  }

    @Override
    public void valuesFromRow(Row row) {
      this.name=row.getString("topology");;
      this.owner = row.getString("owner");
      this.query = row.getString("query_name");
      this.id = row.getLong("id");
      this.processed = row.getDate("update_time");
    }
    
    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
    boundStm.bind(name,id,owner,query);
    
    }

    
    @Override
     public void save() {
     log.warn("Saving topology journal for id "+ id + " topology:"+name + " owner:"+owner +" queryName:"+query);     
     super.save();
   } 
    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
      boundStm.bind(name,id,owner,query);
       log.warn("Binding cql params of new topology journal item for id "+ id + " topology:"+name + " owner:"+owner +" queryName:"+query);     
  
    }
    
  
  
}
