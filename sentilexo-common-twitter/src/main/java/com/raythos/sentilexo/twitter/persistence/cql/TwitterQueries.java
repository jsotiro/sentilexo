/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.twitter.persistence.cql;

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
    private BoundStatement boundOwnerLoadStatement;
    private BoundStatement boundAllLoadStatement;

    
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
        List<TwitterQuery> items=new ArrayList<TwitterQuery>();
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
           TwitterQuery queryObj = new TwitterQuery(session);
           queryObj.valuesFromRow(row);
           items.add(queryObj);
        }
      return items;        
      }
 
    
    
}
