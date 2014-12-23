package com.raythos.messaging.kafka;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import com.raythos.sentilexo.common.utils.AppProperties;
import java.io.Serializable;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author yanni
 */
public class StringTopicMessageProducer  {
       private String topic;
       private String zookeeperHost;
       private Producer<String, String> producer;
       public static String  BROKER_LIST = "metadata.broker.list";
       public static String  REQUEST_ACK = "request.required.acks";
       public String getTopic() {
           return topic;
       }
       public void setTopic(String topic) {
           this.topic = topic;
}
       public String getZookeeperHost() {
           return zookeeperHost;
}
       public void setZookeeperHost(String zookeeperHost) {
           this.zookeeperHost = zookeeperHost;
}
       public void start() {
           Properties props = new Properties();
           props.put("serializer.class", "kafka.serializer.StringEncoder");
           props.put(BROKER_LIST, AppProperties.getProperty(BROKER_LIST));       
           props.put(REQUEST_ACK, AppProperties.getProperty(REQUEST_ACK));
           ProducerConfig config = new ProducerConfig(props);
           this.producer = new Producer<>(config);
}
       public void stop() {
           this.producer.close();
       }

       public  void post(Serializable object) {
           String payload = object.toString();
           postString(payload);
       }
       

        public  void postString(String value) {
           KeyedMessage<String, String> data = new KeyedMessage<>(topic, value);
           this.producer.send( data);
       }

    
       
        public  void postPropertyValuePair(String name, Serializable value) {
           String payload = name+(char)9+value.toString();
           KeyedMessage<String, String> data = new KeyedMessage<>(topic, payload);
           this.producer.send( data);
       }
}
