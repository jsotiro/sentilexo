package com.raythos.sentilexo.streams.twitter;
import com.raythos.sentilexo.twitter.common.domain.TwitterQueryResultItemMapper;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;  
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import java.io.IOException;
import twitter4j.TwitterObjectFactory;
/**
 *
 * @author yanni
 */


public class TwitterQueryListener implements StatusStreamHandler {
     static final Logger log = LoggerFactory.getLogger(TwitterQueryListener.class);
         
     String[] queryTerms;
     String listenerQuery;
     String queryName;
     Date startedTime;
     TopicMessageProducer topic;


    public TopicMessageProducer getTopic() {
        return topic;
    }

    public void setTopic(TopicMessageProducer topic) {
        this.topic = topic;
    }
  
    public String[] getQueryTerms() {
        return queryTerms;
    }

    public void setQueryTerms(String[] queryTerms) {
        this.queryTerms = queryTerms;
    }

    public Date getStartedTime() {
        return startedTime;
    }

    public void setStartedTime(Date startedTime) {
        this.startedTime = startedTime;
    }
     

    @Override
    public void onDisconnectMessage(DisconnectMessage dm) {
        log.warn("Twitter Stream "+ dm.getStreamName()
                +"Disconnected with code"
                + dm.getDisconnectCode()
                + " and reason   "
                + dm.getDisconnectReason());     
    }

    @Override
    public void onStallWarningMessage(StallWarningMessage swm) {
        log.warn( "Stall Warning Message with code{0} and message {1}", new Object[]{swm.getCode(), swm.getMessage()});     
    }

    @Override
    public void onUnknownMessageType(String string) {
        log.warn("Unknown message type with message  "+ string );
    }
       
    public String getListenerQuery() {
        return listenerQuery;
    }

    public void setListenerQuery(String listenerQuery) {
        this.listenerQuery = listenerQuery;
    }

    
    void saveStatus(Status status){
     String s = TwitterObjectFactory.getRawJSON(status);
     TwitterQueryResultItemAvro tqri = TwitterQueryResultItemMapper.mapItem(queryName, listenerQuery, status);
         try {
             byte[] avroData = TwitterQueryResultItemMapper.getAvroSerialized(tqri);
             topic.postBinary(avroData);
         } catch (IOException ex) {
            log.error("error when saving Avro string with message "+ex.getMessage());
         }

    }
    
    @Override
    public void onStatus(Status status) {
       saveStatus(status);
       log.trace("Saved status with Status Id "+status.getId()+ " on kafka topic "+topic.getTopic());     
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {
      log.trace("status deletion notice. Status Id "+ sdn.getStatusId()); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onTrackLimitationNotice(int i) {
        //This notice will be sent each time a limited stream becomes unlimited.
        //If this number is high and or rapidly increasing, it is an indication that your predicate is too broad, and you should consider a predicate with higher selectivity.   
        log.warn("Track limitation notice. Number Of Limited Statuses" + i);        
    }

    @Override
    public void onScrubGeo(long l, long l1) {
       log.warn("onScrubGeo for " 
                  +  l
                  + " and " 
                  + l1 );     
     }

    @Override
    public void onStallWarning(StallWarning sw) {
       log.warn("Stall Warning with code" 
                  + sw.getCode()
                  + " and message " 
                  + sw.getMessage());     
 
    }

    @Override
    public void onException(Exception excptn) {
        log.error("Exception reported",excptn);       
    }

    
    String getQueryName() {
        return queryName; 
    }
    void setQueryName(String queryName) {
        this.queryName = queryName; 
    }
    
}
