package com.raythos.sentilexo.trident.twitter;

/**
 *
 * @author yanni
 */

   

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import com.raythos.sentilexo.utils.AppProperties;
import java.util.List;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.Filter;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author yanni
 */
public class TwitterStreamTridentTopology {
     private static final String POSITIVE_KEYWORDS="positivekeywords";
     private static final String NEGATIVE_KEYWORDS="negativekeywords";
    
     
     public static void main(String[] args) {
        
        String zkhosts = AppProperties.getProperty("zkhosts");
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);
        String topic = AppProperties.getProperty("kafka.topic.results");
        String cqlhost =  AppProperties.getProperty("cqlhost", "localhost");
        String cqlschema = AppProperties.getProperty("cqlschema","twitterqueries");
                        
        TridentKafkaConfig kafkaConfig  = new TridentKafkaConfig(brokerHosts, topic);
        kafkaConfig.forceFromStart = true; //forceStartOffsetTime(readFromMode  /* either earliest or current offset */);
    
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        String[] languagesToAccept = {"en","und"};
        Fields avroObjectFields = new Fields("StatusId","ResultItem","lang");
     //   Fields hashtagsFields = new Fields("queryName","statusId","createdAt","retweet", "hashtags");
        Fields hashtagTotalFields = new Fields("query","sId","hashtag","cAt","retwt");
        Fields counterField = new Fields("count");;
 
        //SentimentAnalyzer.initialiseCoreNLP();
        TwitterDataManager dataMgr =  TwitterDataManager.getInstance();
        dataMgr.connect(cqlhost,cqlschema); 
        List positiveKeywords = dataMgr.getKeywordsList(POSITIVE_KEYWORDS);
        List negativeKeywords =  dataMgr.getKeywordsList(NEGATIVE_KEYWORDS);
        Config config = new Config();
        config.setDebug(true);
        TridentTopology topology = new TridentTopology();

        topology.newStream("twitter-stream", kafkaSpout)
				.parallelismHint(1)
	                        .each(new Fields("bytes"),new DeserializeAvroResultItem(), avroObjectFields)
                                .each(avroObjectFields, new LanguageFilter(languagesToAccept)) 
                                .each(avroObjectFields, new CalculateSimpleSentimentTotals(positiveKeywords, negativeKeywords),CalculateSimpleSentimentTotals.hashtagsFields )
                                .each(CalculateSimpleSentimentTotals.hashtagsFields , new ExtractHashtags(), hashtagTotalFields)
                                .each(hashtagTotalFields, new CalculateHashtagTotals(),counterField);
                                    
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KAFKA_INCOMING_TRIDENT_TWEETS_TOPOLOGY", config, topology.build());
    
        while (true) {
          
        }
        
        
      
    }
    
}
