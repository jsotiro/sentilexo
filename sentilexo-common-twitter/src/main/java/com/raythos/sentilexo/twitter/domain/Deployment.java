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

/**
 *
 * @author yanni
 */
public class Deployment extends PersistedEntity{
  private long deploymentNo = 0L;
  private Date depl_time;
  private String topology;
  
  public Deployment(){
     super(); 
  }
  
  public Deployment(String topology, long deplNo){
     super(); 
     this.topology = topology;
     this.deploymentNo = deplNo;
  }
  
  
  
    public long getDeploymentNo() {
        return deploymentNo;
    }

    public void setDeploymentNo(long deploymentNo) {
        this.deploymentNo = deploymentNo;
    }

    public String getTopology() {
        return topology;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }

    public Date getDepl_time() {
        return depl_time;
    }
  
    @Override
    public void valuesFromRow(Row row) {
       if (row!=null) {
          depl_time = row.getDate("depl_time");
          deploymentNo=row.getLong("depl_id");
       }     
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
          boundStm.bind(topology);
    }

    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
        boundStm.bind(topology, deploymentNo);
    }
    @Override
    public void save(){
       deploymentNo++;
       super.save();
    }  
}
