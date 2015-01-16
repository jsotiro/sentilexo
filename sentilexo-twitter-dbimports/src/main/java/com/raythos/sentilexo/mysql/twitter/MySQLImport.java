/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.mysql.twitter;

import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.common.utils.AppProperties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public class MySQLImport {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
          try {
            MySQLImportWorker updater = new  MySQLImportWorker();
            updater.setQueryName(AppProperties.getProperty("QueryName"));
            TopicMessageProducer  statusTopic = new TopicMessageProducer();           
            statusTopic.setTopic(AppProperties.getProperty("kafka.topic.results"));
            statusTopic.start();
            updater.setTwitterResultItemTopic(statusTopic);
            
//               AppProperties.setPropertiesFile("/etc/raythos/dbutils.properties");
               String dbhost = AppProperties.getProperty("import.dbhost");
               String db = AppProperties.getProperty("import.db");
               String dbuser =  AppProperties.getProperty("import.dbuser");
               String dbpwd= AppProperties.getProperty("import.dbpwd");
               
               updater.connect(dbhost,db,dbuser,dbpwd);
               String cqlhost =AppProperties.getProperty("cqlhost");
               String cqlschema =  AppProperties.getProperty("cqlschema");       
                       
            DataManager.getInstance().connect(cqlhost,cqlschema );
            updater.updateTable();
        } catch (Exception ex) {
            Logger.getLogger(MySQLImportWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
}
