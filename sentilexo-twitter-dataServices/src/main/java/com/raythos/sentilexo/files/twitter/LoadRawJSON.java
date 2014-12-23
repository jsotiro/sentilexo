/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.files.twitter;

import com.raythos.messaging.kafka.StringTopicMessageProducer;
import com.raythos.messaging.kafka.TopicMessageProducer;
import  com.raythos.sentilexo.common.utils.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public final class LoadRawJSON {
  
    
   static Logger log = LoggerFactory.getLogger(LoadRawJSON.class);
    
    public static void main(String[] args) {
       String basePath = "/Users/yanni/sentidata";
        if (args.length > 0) {
           basePath=args[0];    
         } 
        
        TwitterJSONLoaderToKafka loader = new TwitterJSONLoaderToKafka();
        try {
            TopicMessageProducer  statusTopic = new TopicMessageProducer();
            StringTopicMessageProducer  jsonTopic = new StringTopicMessageProducer();
            
            statusTopic.setTopic(AppProperties.getProperty("kafka.topic.results"));
            statusTopic.start();
            loader.setTwitterResultItemTopic(statusTopic);
            
            jsonTopic.setTopic(AppProperties.getProperty("kafka.topic.json"));
            jsonTopic.start();
            loader.setTwitterJsonTopic(jsonTopic);
            
            
           
            loader.setQueryName(AppProperties.getProperty("QueryName"));
            loader.setQueryTerms(AppProperties.getProperty("QueryTerms"));
            log.trace("Stream basic properties loaded and configured from "+AppProperties.getPropertiesFile());
            int result = loader.importFilesFromPath(basePath);
            log.trace(" Files read :"+ loader.getFilesRead());
            log.trace(" Lines read :"+ loader.getLinesRead());
            log.trace(" Statuses read :"+ loader.getStatusesRead());
            System.exit(result);
        } catch (Exception e) {

            log.error("exception with error: " + e.getMessage());
            System.exit(-10);
       }  
    }

    
    
    
}