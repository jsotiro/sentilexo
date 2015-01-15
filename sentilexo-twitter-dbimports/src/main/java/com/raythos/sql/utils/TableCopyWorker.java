package com.raythos.sql.utils;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author yanni
 */
public class TableCopyWorker {
    int batch_size = 1000000;
    long lastId = 0;
    
    String sourceTable;
    String destTable;
    String idColName = "id";
    Connection conn;  
    String jbdcDriver = "com.mysql.jdbc.Driver";
    String dbUrl;
    Statement stmt = null;
    ResultSet rs = null;
  PreparedStatement insertStmt = null ;
    private int count;
    /*private static final SimpleDateFormat dateFormat = 
                         new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
 */
    public String getIdColName() {
        return idColName;
    }

    public void setIdColName(String idColName) {
        this.idColName = idColName;
    }
   
  
 

    public long getLastId() {
        return lastId;
    }

    public void setLastId(long lastId) {
        this.lastId = lastId;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getDestTable() {
        return destTable;
    }

    public void setDestTable(String destTable) {
        this.destTable = destTable;
    }
   
    
    public void connect(String host, String database, String user, String password) throws Exception{
            dbUrl = "jdbc:mysql://"+host+"/"+database;
            Class.forName(jbdcDriver);
           // Open a connection
            conn = DriverManager.getConnection(dbUrl, user, password);
            stmt= conn.createStatement();
    }

    
    public void copyBatch(long Id){
        
    }
    
    long getTargetStartId() {
        long result;
        String sql;
        try {
            sql = "SELECT Max(" +idColName+") FROM "+destTable;
            rs = stmt.executeQuery(sql); 
            rs.first();
        result = rs.getLong(1);
        }
        catch (Exception e) {
            logException(e);
            result =  0;
        }
        if (result == 0) {
            try {
                sql="SELECT Min(" +idColName+") FROM "+sourceTable;
                rs = stmt.executeQuery(sql); 
                rs.first();
                result = rs.getLong(1);
             } 
                catch (Exception e) {
                    logException(e);
                    result =  0;
                }
                
        }
        try {
            rs.close();
          
        } catch (SQLException ex) {
           logException(ex);
        }
         
        return result;
    }
    
    int numColumns;
    int[] columnTypes;
    String[] columnNames;
    String columnNamesSQL;
    String columnValuesSQL;
    String insertStatementSQLTemplate = "Insert into %s (%s) values(%s)";
    String insertSQL;
    
    String  getInsertSQL(ResultSet rs) throws SQLException{
        ResultSetMetaData rsmd = rs.getMetaData();
        numColumns = rsmd.getColumnCount();
        columnTypes = new int[numColumns];
        columnNames = new String[numColumns]; 
        columnNamesSQL = "";
        columnValuesSQL="";
        for (int i = 0; i < numColumns; i++) {
            columnTypes[i] = rsmd.getColumnType(i + 1);
            columnNames[i] =rsmd.getColumnName(i + 1);
            if (i != 0) {
                columnNamesSQL += ",";
                columnValuesSQL += ",";
            }
               columnNamesSQL += columnNames[i];
              columnValuesSQL += "?";
           
          }    
         String result = String.format(insertStatementSQLTemplate, destTable, columnNamesSQL, columnValuesSQL);
         return result;
    }
    
    
    void setInsertValues(ResultSet rs, PreparedStatement prepSQL) throws SQLException{       
      
   //     java.util.Date d = null; 
     

        for (int i = 0; i < numColumns; i++) {
            
           //  prepSQL.setObject(i+1, rs.getObject(i+1));
       
       
            switch (columnTypes[i]) {
              case Types.BIGINT:
                     prepSQL.setLong(i+1, rs.getLong(i+1));
                  break;
              case Types.DOUBLE:
                  prepSQL.setDouble(i+1, rs.getDouble(i+1));
                  break;
              case Types.BIT:
              case Types.INTEGER:
              case Types.SMALLINT:
              case Types.TINYINT:
                    prepSQL.setInt(i+1, rs.getInt(i+1));
                  break;
              case Types.BOOLEAN:
                  prepSQL.setBoolean(i+1, rs.getBoolean(i+1));
                  break;
              case Types.DECIMAL:
              case Types.FLOAT:
                    prepSQL.setFloat(i+1, rs.getFloat(i+1));
                break;

               case Types.DATE:
                    prepSQL.setDate(i+1, rs.getDate(i+1));
                    break;
                case Types.TIME:
                    prepSQL.setTime(i+1, rs.getTime(i+1));
                    break;

                case Types.TIMESTAMP:
                    prepSQL.setTimestamp(i+1, rs.getTimestamp(i+1));
                    break;

                default:
                   prepSQL.setObject(i+1, rs.getObject(i+1));
                   break;
                }
            } 
         
    }
   
   void logException(Exception ex){
      Logger.getLogger(TableCopyWorker.class.getName()).log(Level.SEVERE, ex.getMessage());  
   }
    
   
     void copyTable(){
        count = 0;
        try {
            rs.setFetchSize(batch_size);
            stmt.setFetchSize(batch_size);
            Logger.getLogger(TableCopyWorker.class.getName()).log(Level.INFO, "starting with Id {0}", Long.toString(this.lastId));
            rs = stmt.executeQuery("select * from "+sourceTable +" where "+idColName+">"+lastId +" LIMIT "+batch_size);
            insertSQL = getInsertSQL(rs);
            insertStmt = conn.prepareStatement(insertSQL);
            while (rs!=null){
               copyTableBatch();
               lastId = getTargetStartId();
               rs = stmt.executeQuery("select * from "+this.sourceTable +" where "+this.idColName+">"+this.lastId +" LIMIT "+batch_size);
   
            }
         } catch (SQLException ex) {
           logException(ex);
        }
        finally {
            
           try {
                Logger.getLogger(TableCopyWorker.class.getName()).log(Level.INFO, "{0} records processed ", count);
               if (rs!=null) rs.close();
               if (stmt!=null) stmt.close();
               if (insertStmt!=null) insertStmt.close();
          } catch (SQLException ex) {
               logException(ex);
          }
      
        }
         
         
     } 
   
    void copyTableBatch() throws SQLException {  
        while (rs.next()){
            this.setInsertValues(rs, insertStmt);
            try{
                long id = rs.getLong(idColName);
                Logger.getLogger(TableCopyWorker.class.getName()).log(Level.INFO, "processing id {0}", Long.toString(id));
                insertStmt.execute();
                count++;
                 Logger.getLogger(TableCopyWorker.class.getName()).log(Level.INFO, "Writing record {0} for id {1}", new Object[]{count, Long.toString(id)});
                
                }
               catch (SQLException ex) {
                   logException(ex);
                }
        }
            
        }
      
    
    }
    
    
    
