/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.files.twitter;

import com.raythos.messaging.kafka.StringTopicMessageProducer;
import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.QueryResultItemMapper;
import twitter4j.Status;

/**
 *
 * @author yanni
 */
public class TwitterJSONLoaderToKafka  extends TwitterJSONLoader {
    

    TopicMessageProducer twitterResultItemTopic;
    StringTopicMessageProducer twitterJsonTopic;

    
    
    public TopicMessageProducer getTwitterResultItemTopic() {
        return twitterResultItemTopic;
    }

    public void setTwitterResultItemTopic(TopicMessageProducer twitterResultItemTopic) {
        this.twitterResultItemTopic = twitterResultItemTopic;
    }

    public StringTopicMessageProducer getTwitterJsonTopic() {
        return twitterJsonTopic;
    }

    public void setTwitterJsonTopic(StringTopicMessageProducer twitterJsonTopic) {
        this.twitterJsonTopic = twitterJsonTopic;
    }
  
    
    @Override 
   void handleStatusObject(int lineNo, Status status, String rawJSONLine){
        log.trace("Posting Status with id " +status.getId() +"from File" +filename +" line #"+lineNo);
                try{
                    TwitterQueryResultItemAvro tqri = new TwitterQueryResultItemAvro();
                    
                    tqri = QueryResultItemMapper.mapItem(queryName, queryTerms, status);
                    byte[] data = QueryResultItemMapper.getAvroSerialized(tqri);
                    twitterResultItemTopic.postBinary(data);
                    twitterJsonTopic.postPropertyValuePair(tqri.getStatusId().toString(), rawJSONLine);
                   log.trace("Status with id " +status.getId() +"posted to Kafka topic  from file" + getFilename() +" line #"+lineNo);
                           } 
                catch (Exception e) {
                log.error("error when posting status to Kafka"+e);
     
                }          
   } 
    
    
}