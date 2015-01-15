/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sql.utils;

import com.raythos.sql.utils.person.TwitterUserDetails;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author yanni
 */
public class UpdateExtUserDetailsWorker {
    int batch_size = 1000000;
    long lastId = 0;
    
    Connection conn;  
    String jbdcDriver = "com.mysql.jdbc.Driver";
    String dbUrl;
    Statement stmt = null;
    ResultSet rs = null;
    PreparedStatement updateStmt = null ;
    private int count=0;
    TwitterUserDetails tud = new TwitterUserDetails();
    String updateSQL= "UPDATE TwitterQueryResults SET "
            + "UserLocation=? ,  UserName=? , UserDescription=? , UserUrl=? , UserisProtected=? ,  UserFollowersCount=? , "
            + "UserCreatedAt=? , UserFriendsCount=? , UserListedCount=? , UserStatusesCount=? "
            + "WHERE StatusId = ? ";
    
    
  
    
    public void connect(String host, String database, String user, String password) throws Exception{
            dbUrl = "jdbc:mysql://"+host+"/"+database;
            Class.forName(jbdcDriver);
           // Open a connection
            conn = DriverManager.getConnection(dbUrl, user, password);
            stmt= conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            updateStmt = conn.prepareStatement(updateSQL);
    }
   
    
   void logException(Exception ex){
      Logger.getLogger(UpdateExtUserDetailsWorker.class.getName()).log(Level.SEVERE, ex.getMessage());  
   }
    
   void logInfo(String info){
      Logger.getLogger(UpdateExtUserDetailsWorker.class.getName()).log(Level.INFO,info);  
   }
   
   
    void updateTable() throws Exception{
        count = 0;
        try {
 
            stmt.setFetchSize(batch_size);
            lastId = getInitialId();
           logInfo("starting with Id  "+ Long.toString(this.lastId));
 
            rs = stmt.executeQuery("select * from TwitterQueryResults where  StatusId>"+lastId +" LIMIT "+batch_size);
        
            
            while (rs!=null && lastId>0){
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
                Logger.getLogger(TableCopyWorker.class.getName()).log(Level.INFO, "{0} records processed ", count);
               if (rs!=null) rs.close();
               if (stmt!=null) stmt.close();
               if (updateStmt!=null) updateStmt.close();
          } catch (SQLException ex) {
               logException(ex);
          }
      
        }
         
         
     } 

    private long getInitialId() {
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
    
    
     private void updateBatchUserRecord() throws Exception {
         lastId=0;
         while (rs.next()){
          updateUserRecord();   
         }
         
     }
    
    private void updateUserRecord() {
    
        try {
            long idToUpdate = rs.getLong("StatusId");
            InputStream is = rs.getBlob("userDetails").getBinaryStream();  ///getString("userDetails"");
            String userDetailsStr = IOUtils.toString(is, StandardCharsets.UTF_8);
            logInfo("processing  "+ userDetailsStr);
            tud.parseFromSerialisedTWitter4JObject(userDetailsStr);
            rs.updateString("UserLocation", tud.getLocation());
            rs.updateString("UserName", tud.getName());
            rs.updateString("UserDescription", tud.getDescription());
            rs.updateString("UserUrl", tud.getUrl());
            rs.updateBoolean("UserisProtected", tud.isProtected());
            rs.updateInt("UserFollowersCount", tud.getFollowersCount());
            rs.updateInt("UserFriendsCount", tud.getFriendsCount());
            rs.updateInt("UserListedCount", tud.getListedCount());
            rs.updateInt("UserFavoritesCount", tud.getFavouritesCount());
            rs.updateInt("UserStatusesCount", tud.getStatusesCount());
            rs.updateTimestamp("UserCreatedAt",  new Timestamp(tud.getCreatedAt().getTime()));
            logInfo("starting with Id  "+ Long.toString(this.lastId));
            logInfo("updating row for   "+ idToUpdate);
         
            rs.updateRow();
        
            logInfo("updated row for   "+ idToUpdate);
            count++;
            lastId = idToUpdate;  
         }
        catch (Exception e){
             logException(e);
            }
          
    }
   
}
