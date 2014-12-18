package com.raythos.sentilexo.trident.twitter;

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
import com.raythos.sentilexo.spouts.JSONFileTwitterSpout;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;
import com.raythos.sentilexo.storm.pmml.NaiveBayesPMMLModelLoader;
import com.raythos.sentilexo.trident.twitter.state.QueryStatsCqlStorageConfigValues;
import com.raythos.sentilexo.trident.twitter.state.SentilexoStateFactory;
import com.raythos.sentilexo.twitter.persistence.cql.Deployments;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import com.raythos.sentilexo.utils.AppProperties;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.io.IOException;
import java.util.List;
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
     private static final Fields groupByFields = new Fields("owner", "queryName");
     private static final Fields totalItemsFields = new Fields("totalitems");
     private static final Fields hashtagTotalFields = new Fields("oN","oQ", "statid", "hashtag","cAt","retwt");
     private static final Fields counterField = new Fields("count");
    
    
    static OpaqueTridentKafkaSpout getKafkaSpout(){
        String zkhosts = AppProperties.getProperty("zkhosts");
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);
        String topic = AppProperties.getProperty("kafka.topic.results");                    
        TridentKafkaConfig kafkaConfig  = new TridentKafkaConfig(brokerHosts, topic);
        kafkaConfig.forceFromStart = true; //forceStartOffsetTime(readFromMode  /* either earliest or current offset */);    
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
     
        return kafkaSpout;
     }  
     
     static JSONFileTwitterSpout initJSONFileTwitterSpout() throws IOException{
        JSONFileTwitterSpout spout = new JSONFileTwitterSpout(5);
        spout.getFileWorker().setBasePath("/Users/yanni/sentidata");
        return spout;
    } 
     
     
     static void setupTopology(Stream mainStream) {        
          String cqlhost =  AppProperties.getProperty("cqlhost", "localhost");
          String cqlschema = AppProperties.getProperty("cqlschema","twitterqueries");
          TwitterDataManager dataMgr =  TwitterDataManager.getInstance();
          dataMgr.connect(cqlhost,cqlschema); 
          
          // keyword-based ("bag of words") sentiment classification
          List positiveKeywords = dataMgr.getKeywordsList(POSITIVE_KEYWORDS);
          List negativeKeywords =  dataMgr.getKeywordsList(NEGATIVE_KEYWORDS);
          CalculateSimpleSentimentTotals simpleSentimentFunction = new CalculateSimpleSentimentTotals(positiveKeywords, negativeKeywords);
                  
          // neural network based sentiment calculation
          Properties props = new Properties();          
          // initialise the CoreNLP engine - this takes time do it before the topology is submited.
          CoreNLPSentimentClassifier.getInstance().setup();
          CalculateNLPSentiment neuralNetNLPSentimentAnalysisFunction = new CalculateNLPSentiment();

          
          // setup for the Naive Bayes classification model developed in R and exported using PMML 
          NaiveBayesHandler handler =  NaiveBayesPMMLModelLoader.loadModel(AppProperties.getProperty("bayes-model", "/twitter-sentiment-bayes.xml"));
          CalculatePmmlBayesSentiment bayesModelClassifier = new CalculatePmmlBayesSentiment(handler);
          
          
          TridentState queryStatsState =mainStream.each(DeserializeAvroResultItem.avroObjectFields, new  ExtractStatsFields(),ExtractStatsFields.statsFields )
                            .groupBy(groupByFields)
                            .persistentAggregate(new SentilexoStateFactory(new QueryStatsCqlStorageConfigValues()),
                                                                           ExtractStatsFields.statsFields, 
                                                                           new QueryTotalsAggregator(), 
                                                                           totalItemsFields);
                                         
          
          
            Fields hashtagFields = CalculateSimpleSentimentTotals.hashtagsFields;

            Stream analysisStream = mainStream        
                            .each(DeserializeAvroResultItem.avroObjectFields, new LanguageFilter(languagesToAccept)) 
                            .each(DeserializeAvroResultItem.avroObjectFields,neuralNetNLPSentimentAnalysisFunction ,CalculateNLPSentiment.statusFields)
                            .each(DeserializeAvroResultItem.avroObjectFields, bayesModelClassifier , CalculatePmmlBayesSentiment.statusFields)
                            .each(DeserializeAvroResultItem.avroObjectFields, simpleSentimentFunction,hashtagFields )
                            .each( hashtagFields, new ExtractHashtags(), hashtagTotalFields)
                            .each(hashtagTotalFields, new CalculateHashtagTotals(),counterField);

     }

     public static void main(String[] args)  {
          String topologyName="KAFKA_INCOMING_TRIDENT_TWEETS_TOPOLOGY";
          // read topologies from cassandra, increment and update with timestamp
          // deploymentNo = dataManager.getDeploymentNo()+1;
          Deployments deploymentTracker = Deployments.getInstance();
          deploymentTracker.load();
              
          
        
          
          long deploymentNo = deploymentTracker.getDeploymentNo()+1;
          boolean local = true;
          boolean useKafka = false; 
          if (args != null && args.length > 0) {
            topologyName= args[0];
            local = false;
            if (args.length > 1 && args[1].equalsIgnoreCase("kafka"));
             useKafka = true;   
          }
          
     

        

         try {
            Config config = new Config();
            config.setDebug(true);
            config.setNumWorkers(3);
            TridentTopology topology = new TridentTopology();

            Stream mainStream;
            if (useKafka) { 
                OpaqueTridentKafkaSpout kafkaSpout = getKafkaSpout();
               mainStream = topology.newStream("twitter-stream", kafkaSpout)
                        .parallelismHint(1).
                        each(new Fields("bytes"),
                             new DeserializeAvroResultItem(), 
                             DeserializeAvroResultItem.avroObjectFields).each(DeserializeAvroResultItem.avroObjectFields, new DuplicatesFilter());            }
            else {
                JSONFileTwitterSpout spout = initJSONFileTwitterSpout();
                 mainStream = topology.newStream("twitter-stream", spout)
                        .parallelismHint(1).
                        each(new Fields("bytes"),
                             new DeserializeAvroResultItem(), 
                             DeserializeAvroResultItem.avroObjectFields).each(DeserializeAvroResultItem.avroObjectFields, new DuplicatesFilter()); 
       
            }    
            
       
            setupTopology(mainStream);
       
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
             // dataManager.updateDeploymentNo(DeploymentNo);
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
