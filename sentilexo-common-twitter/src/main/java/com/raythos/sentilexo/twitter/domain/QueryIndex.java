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

/**
 *
 * @author yanni
 */
public class QueryIndex extends PersistedEntity {

    private String owner;
    private String queryName;
    private long id;
    private long addedByDepl;
    private boolean parseData = true;

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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getAddedByDepl() {
        return addedByDepl;
    }

    public void setAddedByDepl(long addedByDepl) {
        this.addedByDepl = addedByDepl;
    }

    public QueryIndex(){
        
    }
    public QueryIndex(String owner, String queryName, long id, long addedByDeplNo){
        super();
        this.owner = owner;
        this.queryName = queryName;
        this.id = id;
        this.addedByDepl = addedByDepl;
    }
    
    public static boolean exists(String owner, String queryName, long id) {
        QueryIndex temp = new QueryIndex(owner,queryName, id,0);
        temp.parseData = false;
        return temp.load();

    }

    @Override
    public void valuesFromRow(Row row) {
        if (parseData) {
            owner = row.getString(0);
            queryName = row.getString(1);
            id = row.getLong(2);
            addedByDepl = row.getLong(3);
        }
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
        boundStm.bind(owner, queryName, id);
    }

    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
        boundStm.bind(owner, queryName, id, addedByDepl);
    }

}
