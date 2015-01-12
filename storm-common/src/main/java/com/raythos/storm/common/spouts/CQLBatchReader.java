/*
 * Copyright 2014 (c) Raythos Interactive Ltd.  http://www.raythos.com
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

package com.raythos.storm.common.spouts;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.persistence.cql.DataConnected;
import com.raythos.sentilexo.persistence.cql.RegisteredPreparedStatements;
import java.util.Iterator;

/**
 *
 * @author yanni
 */
class CQLBatchReader extends DataConnected {
   
    private int batchSize;
    
    private String topologyName;
    private String mainTopologyName;
    private long lastProcessedId;

    private String queryName;
    private String owner;
    
    private boolean finished;
 
     public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

  
    public String getTopologyName() {
        return topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

   public boolean isFinished() {
        return this.finished;
    }
    void startReadingData()  {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    BoundStatement boundReadMainStmt = new BoundStatement(RegisteredPreparedStatements.selectQueriesIdBatch);
    BoundStatement boundReadBacthStmt = new BoundStatement(RegisteredPreparedStatements.selectLatestIdForTopology);
    
    public void getLastProcessedItem(){      
        boundReadMainStmt.bind(topologyName);
        ResultSet result = getSession().execute(boundReadMainStmt);
        setLastResultSet(result);
        boolean found = result.getAvailableWithoutFetching() > 0;
        if (found) {
            Row row = result.all().get(0);
            this.queryName = row.getString("query_name");
            this.owner= row.getString("owner");
        }
        else 
           lastProcessedId = -1;  
    }
    
    public Iterator<Row> readNextBatch() {
        boundReadBacthStmt.bind(mainTopologyName, lastProcessedId, batchSize);
        ResultSet result = getSession().execute(boundReadBacthStmt);
        setLastResultSet(result);
        return result.iterator();
    }
}
