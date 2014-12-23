/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com *
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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.persistence.cql.PersistedEntity;

/**
 *
 * @author yanni
 */
public class TwitterResultJson  extends PersistedEntity {
    private long statusId; 
    private String jsonText;
    private boolean parseData = true; 

    public long getStatusId() {
        return statusId;
    }

    public void setStatusId(long statusId) {
        this.statusId = statusId;
    }

    public String getJsonText() {
        return jsonText;
    }

    public void setJsonText(String jsonText) {
        this.jsonText = jsonText;
    }
    
    
    
    public TwitterResultJson(){
    }
    
    public TwitterResultJson(long statusId, String jsonText){
       super(); 

    }
    
    public static boolean exists(long statusId){
      TwitterResultJson temp = new TwitterResultJson(statusId,null);
      temp.parseData = false;
      return temp.load();
    }
    
    @Override
    public void valuesFromRow(Row row) {
        if (parseData) {  
        this.statusId = row.getLong(0);
        this.jsonText = row.getString(1);
        }     
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
       boundStm.bind(statusId);  
    }

    
    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
        boundStm.bind(statusId,jsonText);
        boundStm.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();   
       }        
}
