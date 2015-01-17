package com.raythos.sentilexo.topologies.base;


/*
 * Copyright 2015 Raythos Interactive Ltd. - http://www.raythos.com .
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
import backtype.storm.LocalCluster;
import com.raythos.sentilexo.common.utils.AppProperties;
import java.io.IOException;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.trident.SendResultItemToKafkaTopic;
import com.raythos.sentilexo.trident.UpdateTopologiesJournal;
import com.raythos.sentilexo.twitter.domain.Deployment;
import com.raythos.sentilexo.twitter.domain.Deployments;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import com.raythos.storm.common.spouts.TextFileBatchedLinesSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;

/**
 *
 * @author John Sotiropoulos
 */

public abstract class CoreTopology  {
    
       protected  static Logger log = LoggerFactory.getLogger(CoreTopology.class);
     
       private  TridentTopology topology = new TridentTopology();
       private  OpaqueTridentKafkaSpout kafkaSpout;     
       private Config config;
       private String topologyName;
 
       private Deployment deploymentTracker;

       
       private int parallelismHint = 1;
       private int workersCount = 3; 
       
       
       private Stream mainStream = null;       
       private int fileBatchSize = 5;
       private String baseFilePath;
       private String fileExtension;
       
       
       private String kafkaHost = "";
       private String sourceTopic = "";
       private String topicToRouteTo="";
       
       
       private boolean useKafka = true;
       private boolean runLocally = true;
       private boolean debug=false;
       
       private BaseFunction binaryDeserializerFunction; 
       private BaseFunction textDeserializerFunction;
       
       public static final Fields itemField = new Fields("ResultItem");
      
   
       
    public  CoreTopology(){
        super();
    }   

    public  CoreTopology(String[] args){
      super();
      configFromArgs(args);
    }
    
    public TridentTopology getTopology() {
        return topology;
    }

    public void setTopology(TridentTopology topology) {
        this.topology = topology;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Deployment getDeploymentTracker() {
        return deploymentTracker;
    }

    public void setDeploymentTracker(Deployment deploymentTracker) {
        this.deploymentTracker = deploymentTracker;
    }

    public int getParallelismHint() {
        return parallelismHint;
    }

    public void setParallelismHint(int parallelismHint) {
        this.parallelismHint = parallelismHint;
    }

    public int getWorkersCount() {
        return workersCount;
    }

    public void setWorkersCount(int workersCount) {
        this.workersCount = workersCount;
    }

    public Stream getMainStream() {
        return mainStream;
    }

    public void setMainStream(Stream mainStream) {
        this.mainStream = mainStream;
    }

    public int getFileBatchSize() {
        return fileBatchSize;
    }

    public void setFileBatchSize(int fileBatchSize) {
        this.fileBatchSize = fileBatchSize;
    }

    public String getBaseFilePath() {
        return baseFilePath;
    }

    public void setBaseFilePath(String baseFilePath) {
        this.baseFilePath = baseFilePath;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public boolean isUsingKafka() {
        return useKafka;
    }

    public void setUseKafka(boolean useKafka) {
        this.useKafka = useKafka;
    }

    public boolean isRunningLocally() {
        return runLocally;
    }

    public void setRunLocally(boolean runLocally) {
        this.runLocally = runLocally;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public BaseFunction getBinaryDeserializerFunction() {
        return binaryDeserializerFunction;
    }

    public void setBinaryDeserializerFunction(BaseFunction binaryDeseserializerFunction) {
        this.binaryDeserializerFunction = binaryDeseserializerFunction;
    }

    public BaseFunction getTextDeserializerFunction() {
        return textDeserializerFunction;
    }

    public void setTextDeserializerFunction(BaseFunction textDeseserializerFunction) {
        this.textDeserializerFunction = textDeseserializerFunction;
    }

    public String getTopicToRouteTo() {
        return topicToRouteTo;
    }

    public void setTopicToRouteTo(String topicToRouteTo) {
        this.topicToRouteTo = topicToRouteTo;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getKafkaHost() {
        return kafkaHost;
    }

    public void setKafkaHost(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }
         
   public OpaqueTridentKafkaSpout getKafkaSpout(){
        String zkhosts = this.getKafkaHost();
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);
        String topic =   this.getSourceTopic();                     
        TridentKafkaConfig kafkaConfig  = new TridentKafkaConfig(brokerHosts, topic);
        kafkaConfig.forceFromStart = true; //forceStartOffsetTime(readFromMode  /* either earliest or current offset */);    
        kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        return kafkaSpout;
     }  
     
     public TextFileBatchedLinesSpout getJSONFileTwitterSpout() throws IOException{
        TextFileBatchedLinesSpout spout = new TextFileBatchedLinesSpout(fileBatchSize);
        spout.getFileWorker().setQueryOwner(AppProperties.getProperty("QueryOwner"));
        spout.getFileWorker().setQueryName(AppProperties.getProperty("QueryName"));
        spout.getFileWorker().setQueryTerms(AppProperties.getProperty("QueryTerms"));
        spout.getFileWorker().setFileExt(fileExtension);
        spout.getFileWorker().setBasePath(baseFilePath);
        
        return spout;
    } 
          

     public void setupMainStream(){
        baseFilePath=AppProperties.getProperty("file.dir.results");
        baseFilePath=AppProperties.getProperty("file.ext.results");
        this.initTopologySourceEndpoint();
        this.initTopologyDestinationEndpoint();
        this.initDeserializerFunctions();
          
          this.kafkaHost = AppProperties.getProperty("zkhosts");
          if (useKafka) { 
            kafkaSpout = getKafkaSpout();
            mainStream = topology.newStream("input-stream", kafkaSpout)
                        .parallelismHint(parallelismHint)
                       .each(kafkaSpout.getOutputFields(),
                             this.binaryDeserializerFunction, itemField);
            }
            else {
              try {
                  TextFileBatchedLinesSpout spout = getJSONFileTwitterSpout();
                  mainStream = topology.newStream("input-stream", spout)
                          .parallelismHint(parallelismHint)
                          .each(spout.getOutputFields(),
                             this.textDeserializerFunction,itemField);
              } catch (IOException ex) {
                  log.error("error whilst processing text file spout. The error was ",ex);
              }
            }     
     }    
       
    
    public void connectToDb(){
          String cqlhost =  AppProperties.getProperty("cqlhost", "localhost");
          String cqlschema = AppProperties.getProperty("cqlschema","twitterqueries");
          DataManager dataMgr =  DataManager.getInstance();
          dataMgr.connect(cqlhost,cqlschema);        
    } 
  
 
    public void setupName(){
       topologyName = this.getClass().getName();
       topologyName = topologyName.replaceAll("\\.", "_"); 
    }
    

    public  void setupTopology() {
         setupName(); 
         connectToDb();
         initTrackingInfo();
         config = new Config();
         config.setDebug(debug);
         config.setNumWorkers(workersCount);
         topology = new TridentTopology();
    }
   
    public  String getTopologyName(){
       return topologyName;         
    };
    
    public  void submitTopology(){
            if (!runLocally) {
                try {
                    StormSubmitter.submitTopology(getTopologyName(), config, topology.build());
                    Thread.sleep(12000); // wait for 2 mins
                }
                catch (AlreadyAliveException | InvalidTopologyException | InterruptedException e) {
                    log.error("Error when submitting the topology. Error Msg: "+e.getMessage());
                }
            }
            else {
                 LocalCluster cluster = new LocalCluster();
                 cluster.submitTopology(getTopologyName(), config, topology.build());
            }
            
         
            if ( runLocally )while (true) {
             // run a loop
            }
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(getTopologyName(), config, topology.build());
    }
   
    public abstract void initDeserializerFunctions();

    public abstract void initTopologySourceEndpoint();
    
    public abstract void initTopologyDestinationEndpoint();
    
    public abstract void defineFunctions();
      /* Override this method to add functions to the topology         
        your main sream is now setup from either the Kafka or Text file sprout
        and the default input field is binary bytes for Kafka or Text Line for files
      */  
     
    
    

     public void initTrackingInfo() {
       // read Deployments from cassandra, increment and update with timestamp           
       deploymentTracker = Deployments.getInstance(topologyName);
       deploymentTracker.load();
    }
     
    public void updateTrackingInfo() {
       deploymentTracker.save();
       Stream trackingStream = mainStream.each(itemField, new UpdateTopologiesJournal(topologyName));
       if (useKafka && topicToRouteTo!=null && !topicToRouteTo.isEmpty())
            trackingStream.each(itemField, new SendResultItemToKafkaTopic(topicToRouteTo));       
    }

    
    public void configFromArgs(String[] args){
        if (args != null){
            runLocally = false;
            if (args.length > 0) {
              if ( ! args[0].equals("+"))
                topologyName= args[0];
          }
          if (args.length > 1) {
              this.useKafka = args[1].toLowerCase().equals("kafka");
          }
  
        }
    } 
    
    public void  execute(){
        setupTopology();
        setupMainStream();
        defineFunctions();
        updateTrackingInfo();
        submitTopology();        
    }
    
    public static void main(String[] args)  {

    }

    
   
      
}
