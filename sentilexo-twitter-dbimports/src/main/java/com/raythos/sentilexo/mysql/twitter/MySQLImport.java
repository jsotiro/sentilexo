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
            
               AppProperties.setPropertiesFile("/etc/raythos/dbutils.properties");
                updater.connect(AppProperties.getProperty("dbhost"),
                                  AppProperties.getProperty("db"), 
                                  AppProperties.getProperty("dbuser"), 
                                  AppProperties.getProperty("dbpwd"));
    
            DataManager.getInstance().connect("localhost", "sentilexo");
            updater.updateTable();
        } catch (Exception ex) {
            Logger.getLogger(MySQLImportWorker.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
}
