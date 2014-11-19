package com.raythos.sentilexo.trident.twitter;

/**
 *
 * @author yanni
 */

   

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import com.raythos.sentilexo.utils.AppProperties;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author yanni
 */
public class OldTwitterStreamTridentTopology {
     private static final String POSITIVE_KEYWORDS="positivekeywords";
     private static final String NEGATIVE_KEYWORDS="negativekeywords";
      protected  static Logger log = LoggerFactory.getLogger(OldTwitterStreamTridentTopology.class);
     
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
        
     //   Fields hashtagsFields = new Fields("queryName","statusId","createdAt","retweet", "hashtags");
        Fields hashtagTotalFields = new Fields("query","sId","hashtag","cAt","retwt");
        Fields counterField = new Fields("count");
 
       
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
	                        .each(new Fields("bytes"),new DeserializeAvroResultItem(), DeserializeAvroResultItem.avroObjectFields)
                                .each(DeserializeAvroResultItem.avroObjectFields, new LanguageFilter(languagesToAccept)) 
                                .each(DeserializeAvroResultItem.avroObjectFields, new CalculateSimpleSentimentTotals(positiveKeywords, negativeKeywords),CalculateSimpleSentimentTotals.hashtagsFields )
                                .each(CalculateSimpleSentimentTotals.hashtagsFields , new ExtractHashtags(), hashtagTotalFields)
                                .each(hashtagTotalFields, new CalculateHashtagTotals(),counterField);
                                    
        
        
         if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, topology.build());
                Thread.sleep(12000); // wait for 2 mins
            }
            catch (AlreadyAliveException | InvalidTopologyException | InterruptedException e) {
                log.error("Error when submitting the topology. Error Msg: "+e.getMessage());
                }
            }
            else {
                LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KAFKA_INCOMING_TRIDENT_TWEETS_TOPOLOGY", config, topology.build());
    
            while (true) {
          
            }
    
      
     }
     }    
}
