/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.mysql.twitter;

import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.QueryResultItemMapper;
import com.raythos.sentilexo.twitter.domain.ResultItem;
import com.raythos.sql.utils.person.TwitterUserDetails;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public class MySQLImportWorker {
    private  int batch_size = 1000;
    private long lastId = 0;
    
    private Connection conn;  
    private final String jbdcDriver = "com.mysql.jdbc.Driver";
    private String dbUrl;
    private Statement stmt = null;
    private ResultSet rs = null;
    private PreparedStatement updateStmt = null ;
    private int count=0;
    private TwitterUserDetails tud = new TwitterUserDetails();
    private long initialId=-1;
    private String queryName; 
    private  TopicMessageProducer twitterResultItemTopic;

    public TopicMessageProducer getTwitterResultItemTopic() {
        return twitterResultItemTopic;
    }

    public void setTwitterResultItemTopic(TopicMessageProducer twitterResultItemTopic) {
        this.twitterResultItemTopic = twitterResultItemTopic;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }
   
    
    public void connect(String host, String database, String user, String password) throws Exception{
            dbUrl = "jdbc:mysql://"+host+"/"+database;
            Class.forName(jbdcDriver);
           // Open a connection
            conn = DriverManager.getConnection(dbUrl, user, password);
            stmt= conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            
     }
   
    
   void logException(Exception ex){
      Logger.getLogger(MySQLImportWorker.class.getName()).log(Level.SEVERE, ex.getMessage());  
   }
    
   void logInfo(String info){
      Logger.getLogger(MySQLImportWorker.class.getName()).log(Level.INFO,info);  
   }
   
   
    void updateTable() throws Exception{
        count = 0;
        try {
 
           stmt.setFetchSize(batch_size);
           lastId = getInitialId();
           logInfo("starting with Id  "+ Long.toString(this.lastId));
 
           rs = stmt.executeQuery("select * from TwitterQueryResults where  StatusId>"+lastId +" LIMIT "+batch_size);
        
            while (rs!=null && lastId!=0){
                updateBatchUserRecord();
                logInfo("--------------"+ count + " records updated");
                if (lastId>0) { 
                    logInfo("retrieving the next "+ batch_size+ "starting from StatusId "+Long.toString(this.lastId));
                    rs = stmt.executeQuery("select * from TwitterQueryResults where  StatusId>"+lastId +" LIMIT "+batch_size);
                    
                }
            }
         } catch (SQLException ex) {
           logException(ex);
        }
        finally {
            
           try {
                Logger.getLogger(MySQLImportWorker.class.getName()).log(Level.INFO, "{0} records processed ", count);
               if (rs!=null) rs.close();
               if (stmt!=null) stmt.close();
               if (updateStmt!=null) updateStmt.close();
          } catch (SQLException ex) {
               logException(ex);
          }
      
        }
         
         
     } 

      
    public long getInitialId(){
        return initialId;
    } 
   
    public void setInitialId(long value){
        initialId=value;
    }
    
    /* not needed as the -1 will do the same job and return from the lowest staus id
     public void resetInitialId() {
         initialId = getFirstInitialId();
     }
     
    
    public long getFirstInitialId() {          
        long result;
        try {
             String sql="SELECT Min(StatusId) FROM TwitterQueryResults";
             rs = stmt.executeQuery(sql); 
             rs.first();
                result = rs.getLong(1);
             } 
             catch (Exception e) {
                    logException(e);
                    result =  0;
            }        
            finally {
                try {
                  rs.close();
          
                    } catch (SQLException ex) {
                logException(ex);
                }
        
            }
          return result;
    }
    
    */
     private void updateBatchUserRecord() throws Exception {
         lastId=0;
         while (rs.next()){
          updateUserRecord();   
         }
         
     }
     
     
     
     
    
    private void updateUserRecord() {
    
        try {
            long idToUpdate = rs.getLong("StatusId");
            // @todo see if status id is in cassandra 
            if (ResultItem.exists(idToUpdate)){
               logInfo(idToUpdate + " already exists in cassandra table");
               }
               else
                {
               // if not  reconstruct the Avro object and post it to kafka
                TwitterQueryResultItemAvro item = MySQLToAvroMapper.mapItem(queryName,rs);
                byte[] data = QueryResultItemMapper.getAvroSerialized(item);
                twitterResultItemTopic.postBinary(data);
            
               logInfo("updated "+ idToUpdate);
            }
            count++;
            lastId = idToUpdate;
            
         }
        catch (Exception e){
             logException(e);
            }
          
    }
   
}
    
    
 