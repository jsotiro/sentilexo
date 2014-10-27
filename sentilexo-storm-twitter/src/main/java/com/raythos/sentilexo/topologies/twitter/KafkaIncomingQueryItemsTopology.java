package com.raythos.sentilexo.topologies.twitter;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.bolts.twitter.DeserialiseTwitterResultItemBolt;
import com.raythos.sentilexo.bolts.twitter.TwitterJSONCQLPersistenceBolt;
import com.raythos.sentilexo.bolts.twitter.TwitterResultsCQLPersistenceBolt;
import com.raythos.sentilexo.bolts.twitter.hashtags.HashTagAggregationsBolt;
import com.raythos.sentilexo.bolts.twitter.hashtags.HashtagPreprocessorBolt;
import java.util.UUID;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author yanni
 */
public class KafkaIncomingQueryItemsTopology {
     private static final String TOPIC_NAME="twitter8";
     private static final String TOPIC_NAME_JSON="json4";
//externalise topic name. zkhosts, 
    
     
     public static void main(String[] args) {
        
         
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");
        
        SpoutConfig kafkaConfig =  new SpoutConfig(brokerHosts, TOPIC_NAME, "/" + TOPIC_NAME, UUID.randomUUID().toString());
        kafkaConfig.forceFromStart = true; //forceStartOffsetTime(readFromMode  /* either earliest or current offset */);
     
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        DeserialiseTwitterResultItemBolt deserialiserBolt = new DeserialiseTwitterResultItemBolt(); 
        TwitterResultsCQLPersistenceBolt persistCQLBolt = new TwitterResultsCQLPersistenceBolt();
        HashtagPreprocessorBolt hashtagpreproBolt = new HashtagPreprocessorBolt();
        HashTagAggregationsBolt hashtagTotalsBolt = new HashTagAggregationsBolt();
        
        
        SpoutConfig jsonKafkaConfig =  new SpoutConfig(brokerHosts, TOPIC_NAME_JSON, "/" + TOPIC_NAME_JSON, UUID.randomUUID().toString());
        jsonKafkaConfig.forceFromStart = true;
        KafkaSpout jsonKafkaSpout = new KafkaSpout(jsonKafkaConfig);
        jsonKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 
        TwitterJSONCQLPersistenceBolt logJsonCQLBolt = new TwitterJSONCQLPersistenceBolt();
     
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("messages", kafkaSpout, 1);
        builder.setSpout("json", jsonKafkaSpout, 1);
        
        
        builder.setBolt("deserialise", deserialiserBolt).shuffleGrouping("messages");
        builder.setBolt("persist", persistCQLBolt).shuffleGrouping("deserialise");
        builder.setBolt("ht-preprocess", hashtagpreproBolt).shuffleGrouping("persist");
        builder.setBolt("ht-total", hashtagTotalsBolt).fieldsGrouping("ht-preprocess",new Fields("statusId","hashtag"));
        builder.setBolt("logJson", logJsonCQLBolt).shuffleGrouping("json");
       
        Config config = new Config();
        config.setDebug(true);
        
        LocalCluster cluster = new LocalCluster();
        //SentimentAnalyzer.initialiseCoreNLP();
        cluster.submitTopology("KAFKA_INCOMING_TWEETS_TOPOLOGY", config, builder.createTopology());
    
        while (true) {
          
        }
        
        
      
    }
    
}
