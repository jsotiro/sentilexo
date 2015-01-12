/*
 * Copyright 2015 John Sotiropoulos .
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
package com.raythos.messaging.kafka;


import java.util.HashMap;

/**
 *
 * @author John Sotiropoulos
 */
public class TopicProducerManager {
   static HashMap<String, TopicMessageProducer> producers = new HashMap<>();
 
   private TopicProducerManager(){       
   }
   
  public static TopicMessageProducer getProducer(String topic){
    return producers.get(topic);
  }
  
  
  
  public static boolean producerExists(String topic){
   return  producers.containsKey(topic);
  }    
  
  
  
  public static TopicMessageProducer getProducerInitIfNeeded(String topic){
    if (producers.containsKey(topic)) 
        return producers.get(topic);
    else
      return initProducer(topic);
  }
          
  public static TopicMessageProducer initProducer(String topic){
       TopicMessageProducer msgProducer = new TopicMessageProducer();
       msgProducer.setTopic(topic);
       msgProducer.start();  
       producers.put(topic, msgProducer);
       return msgProducer;
  }
  
}
