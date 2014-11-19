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
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class TwitterQuery {
    
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
  private PreparedStatement loadQuery;
  private PreparedStatement saveQuery;  
  BoundStatement boundLoadStatement;
  BoundStatement boundSaveStatement; 
  
  
    public TwitterQuery(Session session){
        super();
        setSession(session);
         
    }
  
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
    

    public void setSession(Session session) {
        this.session = session;
        loadQuery = session.prepare("select * from queries where owner=? and queryName=?");
        saveQuery = session.prepare("INSERT INTO queries(owner,   queryname,   active, queryterms, connectionparams, totalitems,earliest, latest, minid, maxid) values (?,?,?,?,?,?,?,?,?,?)");
         boundLoadStatement = new BoundStatement(loadQuery);
         boundSaveStatement = new BoundStatement(saveQuery);

    }
    
   public boolean load(){
   boolean found;
   boundLoadStatement.bind(owner,queryName);
   ResultSet results = session.execute(boundLoadStatement);
   found = results.getAvailableWithoutFetching() > 0;
   if (found) {
        Row row = results.all().get(0);
        valuesFromRow(row);
    }     
   return found;
   }
   
   
   
   
   public void save() {        
      boundSaveStatement.bind(owner,queryName,active,queryTerms,connectionParams,totalItems,earliest, latest, minId, maxId);
      ResultSet results = session.execute(boundSaveStatement);
     }
}
