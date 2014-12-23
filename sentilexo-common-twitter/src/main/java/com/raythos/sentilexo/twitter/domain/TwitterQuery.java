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
import com.datastax.driver.core.Session;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.persistence.cql.PersistedEntity;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class TwitterQuery extends PersistedEntity{
    
  private String owner;
  private String queryName;
  private  boolean active;
  private List<String> queryTerms;
  private Map<String, String> connectionParams;
  private int totalItems;
  private Date earliest;
  private Date latest;
  private long minId;
  private long maxId;   
  private Session session;
   
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public List<String> getQueryTerms() {
        return queryTerms;
    }

    public void setQueryTerms(List<String> queryTerms) {
        this.queryTerms = queryTerms;
    }

    public Map<String, String> getConnectionParams() {
        return connectionParams;
    }

    public void setConnectionParams(Map<String, String> connectionParams) {
        this.connectionParams = connectionParams;
    }

    public int getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(int totalItems) {
        this.totalItems = totalItems;
    }

    public Date getEarliest() {
        return earliest;
    }

    public void setEarliest(Date earliest) {
        this.earliest = earliest;
    }

    public Date getLatest() {
        return latest;
    }

    public void setLatest(Date latest) {
        this.latest = latest;
    }

    public long getMinId() {
        return minId;
    }

    public void setMinId(long minId) {
        this.minId = minId;
    }

    public long getMaxId() {
        return maxId;
    }

    public void setMaxId(long maxId) {
        this.maxId = maxId;
    }

    public Session getSession() {
        return session;
    }

    
  @Override
   public void valuesFromRow(Row row){
        owner = row.getString(0);
        queryName = row.getString(1);
        active = row.getBool(2);
        connectionParams = row.getMap(3, String.class, String.class);
        earliest = row.getDate(4);
        latest = row.getDate(5);
        maxId = row.getLong(6);   
        minId = row.getLong(7);
        queryTerms = row.getList(8, String.class);
        totalItems = row.getInt(9);

    }
 
          
  @Override
   public void bindCQLLoadParameters(BoundStatement boundStm) {
       boundStm.bind(owner,queryName);   
   } 
    
  @Override
   public void bindCQLSaveParameters(BoundStatement boundStm) {
       boundStm.bind(owner,queryName,active,queryTerms,connectionParams,totalItems,earliest, latest, minId, maxId);
   } 
      
}
