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

import com.raythos.sentilexo.twitter.domain.TwitterQuery;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author yanni
 */
public class TwitterQueries {
    
    private Session session;

    private PreparedStatement loadOwnerQuery;
    private PreparedStatement loadAllQuery;
    private BoundStatement   boundOwnerLoadStatement;
    private BoundStatement   boundAllLoadStatement;

    
    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
        loadOwnerQuery = session.prepare("select * from queries where owner=?");
        loadAllQuery = session.prepare("select * from queries");
        boundOwnerLoadStatement = new BoundStatement(loadOwnerQuery);
        boundAllLoadStatement = new BoundStatement(loadAllQuery);
 
    }
    
    public TwitterQueries(Session session){
        super();
        setSession(session);
    }
    
                        
    public List<TwitterQuery>getQueries(String owner) {
        List<TwitterQuery> items=new ArrayList<>();
        BoundStatement stmt;
        if (owner==null || owner.isEmpty())
           stmt = boundAllLoadStatement;
         else
           {  
           stmt = boundOwnerLoadStatement;
           stmt.bind(owner);
           }
        ResultSet results = session.execute(stmt);
        for (Row row : results){
           TwitterQuery queryObj = new TwitterQuery();
           queryObj.valuesFromRow(row);
           items.add(queryObj);
        }
      return items;        
      }
 
    
    
}
