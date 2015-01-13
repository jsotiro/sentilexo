/*
 * Copyright 2014 (c) Raythos Interactive Ltd.  http://www.raythos.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.raythos.sentilexo.trident;

import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.messaging.kafka.TopicProducerManager;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import com.raythos.sentilexo.twitter.domain.ResultItem;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class SendResultItemToKafkaTopic extends BaseFilter {
   protected  static Logger log = LoggerFactory.getLogger(SendResultItemToKafkaTopic.class);
   private String topicName;
   
   
   public SendResultItemToKafkaTopic(String topicName) {
       this.topicName = topicName;
      TopicProducerManager.getProducerInitIfNeeded(topicName);     
  }

  @Override
  public boolean isKeep(TridentTuple tuple) {
    TopicMessageProducer topicSender = TopicProducerManager.getProducerInitIfNeeded(topicName);
    Map item =  (Map)tuple.get(0);
    long id = (long)item.get(QueryResultItemFieldNames.STATUS_ID);
     // @TODO replace above with Avro object when serialsation works
     //  TwitterQueryResultItemAvro item =  (TwitterQueryResultItemAvro)tuple.get(0);
     ResultItem dataItem = new ResultItem(item);
     
     byte[] buffer;
     buffer = ResultItem.toBinaryJSon(dataItem);
     topicSender.postBinary(buffer);
     log.info("Item with"+ id + "id was posted to topic " + topicName);
     return true;                
  }

    
}
