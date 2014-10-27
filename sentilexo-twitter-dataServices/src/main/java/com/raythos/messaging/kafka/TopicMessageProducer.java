package com.raythos.messaging.kafka;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import com.raythos.sentilexo.utils.AppProperties;
import java.io.Serializable;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author yanni
 */


public class TopicMessageProducer  {
       private String topic;
       private String zookeeperHost;
        Producer producer;
       public static String  BROKER_LIST = "metadata.broker.list";
       public static String  REQUEST_ACK = "request.required.acks";
       Properties props = new Properties();
       ProducerConfig config;
      
       public Properties getProps() {
           return props;
       }
       
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
       
        public void setSerializer(){
           this.getProps().put("serializer.class", "kafka.serializer.DefaultEncoder");
         }
       
        
       public Producer getProducer(){
          return producer; 
       }  
        
      public void createProducer(){
            this.producer = new Producer<>(config);
       }  
       
       
       public void start() {
          setSerializer();
           props.put("serializer.class", "kafka.serializer.DefaultEncoder");
           props.put(BROKER_LIST, AppProperties.getProperty(BROKER_LIST));       
           props.put(REQUEST_ACK, AppProperties.getProperty(REQUEST_ACK));
           config = new ProducerConfig(props);
           createProducer();

}
       public void stop() {
           this.producer.close();
       }

       public  void post(Serializable object) {
           String payload = object.toString();
           postString(payload);
       }

       public void postBinary(byte[] value) {
           KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, value);
           this.producer.send(data);
       }
 

        public  void postString(String value) {
           KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, value.getBytes());
           this.producer.send( data);
       }
    
        public  void postPropertyValuePair(String name, Serializable value) {
           String payload = name+(char)9+value.toString();
           KeyedMessage<String, byte[]> data = new KeyedMessage<>(topic, payload.getBytes());
           this.producer.send(data);
       }
}
