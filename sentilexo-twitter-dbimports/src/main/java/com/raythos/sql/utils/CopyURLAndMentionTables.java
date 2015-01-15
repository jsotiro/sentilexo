/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sql.utils;

import com.raythos.sentilexo.common.utils.AppProperties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public class CopyURLAndMentionTables {

    /**
     * @param args the command line arguments
     */

    
     static void  copyTable(String source, String dest) {
       TableCopyWorker tblCopier = new TableCopyWorker();
        tblCopier.setSourceTable(source);
        tblCopier.setDestTable(dest);
    
            try {
            AppProperties.setPropertiesFile("/etc/raythos/dbutils.properties");
                tblCopier.connect(AppProperties.getProperty("dbhost"),
                                  AppProperties.getProperty("db"), 
                                  AppProperties.getProperty("dbuser"), 
                                  AppProperties.getProperty("dbpwd"));
    
            long id = tblCopier.getTargetStartId();
            tblCopier.setLastId(id);
            tblCopier.copyTable();
        } catch (Exception ex) {
                Logger.getLogger(CopyURLAndMentionTables.class.getName()).log(Level.SEVERE, null, ex);
        }

     }
    
    public static void main(String[] args)  {
        // TODO code application logic here
         
        copyTable("UserMentions","StatusUserMentions");                
        copyTable("URLS","StatusURLS");

        
    }
    
}
