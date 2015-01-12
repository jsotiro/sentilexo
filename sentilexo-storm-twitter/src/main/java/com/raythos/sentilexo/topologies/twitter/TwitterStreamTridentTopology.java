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


package com.raythos.sentilexo.topologies.twitter;

/**
 *
 * @author yanni
 */

  
import com.raythos.sentilexo.trident.twitter.aggregations.QueryTotalsAggregator;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;
import com.raythos.sentilexo.trident.twitter.state.QueryStatsCqlStorageConfigValues;
import com.raythos.sentilexo.state.SentilexoStateFactory;
import com.raythos.sentilexo.twitter.domain.Deployment;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.common.utils.AppProperties;
import com.raythos.sentilexo.trident.twitter.hashtags.CalculateHashtagTotals;
import com.raythos.sentilexo.trident.twitter.sentiment.CalculateNLPSentiment;
import com.raythos.sentilexo.trident.twitter.hashtags.CalculateSimpleSentimentTotals;
import com.raythos.sentilexo.trident.twitter.sentiment.CoreNLPSentimentClassifier;
import com.raythos.sentilexo.trident.twitter.AvroBytesToResultItemFunction;
import com.raythos.sentilexo.trident.twitter.sentiment.DirectCalculatePmmlBayesSentiment;
import com.raythos.sentilexo.trident.twitter.DuplicatesFilter;
import com.raythos.sentilexo.trident.twitter.hashtags.ExtractHashtags;
import com.raythos.sentilexo.trident.twitter.ExtractStatsFields;
import com.raythos.sentilexo.trident.twitter.LanguageFilter;
import com.raythos.sentilexo.trident.SendResultItemToKafkaTopic;
import com.raythos.sentilexo.trident.twitter.TextLineToResultItemFunction;
import com.raythos.sentilexo.trident.UpdateTopologiesJournal;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.DefaultSetting;
import com.raythos.sentilexo.twitter.domain.Deployments;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import com.raythos.storm.common.spouts.TextFileBatchedLinesSpout;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

public class TwitterStreamTridentTopology {
     private static final String POSITIVE_KEYWORDS="positivekeywords";
     private static final String NEGATIVE_KEYWORDS="negativekeywords";
     protected  static Logger log = LoggerFactory.getLogger(TwitterStreamTridentTopology.class);
    
     private static final String[] languagesToAccept = {"en","und"};
     private static final Fields groupByFields = new Fields(QueryResultItemFieldNames.QUERY_OWNER, QueryResultItemFieldNames.QUERY_NAME);
     private static final Fields totalItemsFields = new Fields("totalitems");
     private static final Fields avroField = new Fields("item");
     
     
     private static final Fields hashtagTotalFields = ExtractHashtags.hashtagTotalsFields;
     private static final Fields counterField = new Fields("count");
     
     
     
     private static final Fields itemField = new Fields("ResultItem");
   
     
    static OpaqueTridentKafkaSpout getKafkaSpout(){
        String zkhosts = AppProperties.getProperty("zkhosts");
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);
        String topic = AppProperties.getProperty("kafka.topic.results");                    
        TridentKafkaConfig kafkaConfig  = new TridentKafkaConfig(brokerHosts, topic);
        kafkaConfig.forceFromStart = true; //forceStartOffsetTime(readFromMode  /* either earliest or current offset */);    
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
     
        return kafkaSpout;
     }  
     
     static TextFileBatchedLinesSpout initJSONFileTwitterSpout() throws IOException{
        TextFileBatchedLinesSpout spout = new TextFileBatchedLinesSpout(5);
        spout.getFileWorker().setQueryOwner("raythos");
        spout.getFileWorker().setQueryName(AppProperties.getProperty("QueryName"));
        spout.getFileWorker().setQueryTerms(AppProperties.getProperty("QueryTerms"));
        spout.getFileWorker().setFileExt(".json");
        spout.getFileWorker().setBasePath("/Users/yanni/sentidata");
        
        return spout;
    } 
     
     
     
     static void caclQueryStats(Stream dataStream){
        TridentState queryStatsState =dataStream   //.each(itemField, new  ExtractStatsFields(),ExtractStatsFields.statsFields )
                            .groupBy(groupByFields)
                            .persistentAggregate(new SentilexoStateFactory(new QueryStatsCqlStorageConfigValues()),
                                                                           ExtractStatsFields.statsFields, 
                                                                           new QueryTotalsAggregator(), 
                                                                           totalItemsFields);
           
     }
     
     
     static void calcHashTagStats(Stream dataStream) {
        DefaultSetting positiveKeywords = new DefaultSetting(POSITIVE_KEYWORDS, null);
        positiveKeywords.load();
        DefaultSetting negativeKeywords = new DefaultSetting(NEGATIVE_KEYWORDS, null);
        negativeKeywords.load();
        CalculateSimpleSentimentTotals simpleSentimentFunction = new CalculateSimpleSentimentTotals(positiveKeywords.getValues(), negativeKeywords.getValues());
        Fields hashtagFields = CalculateSimpleSentimentTotals.hashtagFields;
        
        Stream analysisStream = dataStream        
                .each(itemField, simpleSentimentFunction,hashtagFields )
                .each(new Fields(itemField.get(0),hashtagFields.get(0)), new ExtractHashtags(), hashtagTotalFields)
                .each(hashtagTotalFields, new CalculateHashtagTotals(),counterField);
          

     }
     static void setupTopology(Stream mainStream) {        
           
          
                            
          // neural network based sentiment calculation
          Properties props = new Properties();          
          // initialise the CoreNLP engine - this takes time do it before the topology is submited.
          CoreNLPSentimentClassifier.getInstance().setup();
          CalculateNLPSentiment neuralNetNLPSentimentAnalysisFunction = new CalculateNLPSentiment();
          
          // setup for the Naive Bayes classification model developed in R and exported using PMML 
          String pmmlFileName = AppProperties.getProperty("bayes-model", "twitter-sentiment-bayes.xml");
          NaiveBayesHandler handler= null;//NaiveBayesPMMLModelLoader.loadModel(pmmlFileName);
          DirectCalculatePmmlBayesSentiment bayesModelClassifier = new DirectCalculatePmmlBayesSentiment(handler);
         /*try {
             PmmlModel model = new PmmlModel(pmmlFileName);
             PmmlModelEvaluationFunction calcPmml = new PmmlModelEvaluationFunction();
             calcPmml.setEvaluator(model.getModelEvaluator());
             
         } catch (Exception ex) {
            log.error("Error loading PMML model " + pmmlFileName + " .The error was", ex);
         }
           */   
          
        
          
        

            Stream nlpSentimentStream = mainStream
                   .each(itemField,neuralNetNLPSentimentAnalysisFunction ,new Fields("NLPSentiment"));
            Stream bayesSentimentStream = mainStream
            .each(itemField, bayesModelClassifier , new Fields("BayesSentiment"));

 
            
       

     }

     public static void main(String[] args)  {
          String topologyName="INCOMING__TWEETS_TRIDENT_TOPOLOGY";
          
          String cqlhost =  AppProperties.getProperty("cqlhost", "localhost");
          String cqlschema = AppProperties.getProperty("cqlschema","twitterqueries");
          DataManager dataMgr =  DataManager.getInstance();
          dataMgr.connect(cqlhost,cqlschema);
          DefaultSetting mainTopologyName = new DefaultSetting("main_topology", null );
          mainTopologyName.addValue(topologyName);
          mainTopologyName.save();
          // read topologies from cassandra, increment and update with timestamp
          Deployment deploymentTracker = Deployments.getInstance(topologyName);
          deploymentTracker.load();
          boolean local = true;
          boolean useKafka = true;
          
          
          if (args != null && args.length > 0) {
            topologyName= args[0];
            local = false;
            if (args.length > 1 && args[1].equalsIgnoreCase("kafka"));
             useKafka = true;   
          }
          
    
         try {
            Config config = new Config();
            config.registerSerialization(TwitterQueryResultItemAvro.class);
            config.setDebug(true);
            config.setNumWorkers(3);
            TridentTopology topology = new TridentTopology();

            Stream mainStream;
            if (useKafka) { 
                OpaqueTridentKafkaSpout kafkaSpout = getKafkaSpout();
               mainStream = topology.newStream("twitter-stream", kafkaSpout)
                        .parallelismHint(20).
                        each(kafkaSpout.getOutputFields(),
                             new AvroBytesToResultItemFunction(), itemField);
            }
            else {
                TextFileBatchedLinesSpout spout = initJSONFileTwitterSpout();
                 mainStream = topology.newStream("twitter-stream", spout)
                        .parallelismHint(1).
                        each(spout.getOutputFields(),
                             new TextLineToResultItemFunction(),itemField);
                 
            }   
                 
                 Stream queryStream = mainStream.each(itemField, new LanguageFilter(languagesToAccept))
                               .each(itemField, new DuplicatesFilter(topologyName))
                               .each(itemField, new UpdateTopologiesJournal(topologyName));
                 if (useKafka)
                      queryStream.each(itemField, new SendResultItemToKafkaTopic(AppProperties.getProperty("kafka.topic.analytics")));
 
                 
                 
                  TridentState queryStatsState =  
                                            queryStream.each(itemField, new  ExtractStatsFields(),ExtractStatsFields.statsFields )
                                            .groupBy(groupByFields)
                                           .persistentAggregate(new SentilexoStateFactory(new QueryStatsCqlStorageConfigValues()),
                                                                           ExtractStatsFields.statsFields, 
                                                                           new QueryTotalsAggregator(), 
                                                                           totalItemsFields); 
           
             
            
 /*           Stream filteredStream = mainStream
                                    .each(itemField, new DuplicatesFilter(topologyName))
                                    .each(itemField, new LanguageFilter(languagesToAccept)) ;
*/
   //         caclQueryStats(filteredStream);
     //       calcHashTagStats(filteredStream);
            //filteredStream.each(itemField, new UpdateTopologiesJournal(topologyName));
        //    if (useKafka)
          //       filteredStream.each(itemField, new SendResultItemToKafkaTopic(AppProperties.getProperty("kafka.topic.analytics")));
            
            
            
            
            //setupTopology(mainStream);
       
            if (!local) {
            try {
                StormSubmitter.submitTopology(topologyName, config, topology.build());
                Thread.sleep(12000); // wait for 2 mins
            }
            catch (AlreadyAliveException | InvalidTopologyException | InterruptedException e) {
                log.error("Error when submitting the topology. Error Msg: "+e.getMessage());
                }
            }
            else {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(topologyName, config, topology.build());
            
           
            }
            
            deploymentTracker.save();
            if ( local )while (true) {
             // run a loop
            }
    
      
        }
         
        
        catch (IOException e) {
                log.error("exception with error: " + e.getMessage());
                System.exit(-10);
                System.exit(-1);      
               }
     }    
}
