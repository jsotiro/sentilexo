package com.raythos.sentilexo.streams.twitter;


import com.google.common.collect.Lists;
import com.raythos.messaging.kafka.TopicMessageProducer;
import com.raythos.sentilexo.utils.AppProperties;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.conf.ConfigurationBuilder;

public final class TwitterQueryStream implements  Serializable {
    private static final long serialVersionUID = 1L;
    private String oAuthConsumerKey;
    private String oAuthConsumerSecret;  
    private String oAuthAccessToken;
    private String oAuthAccessTokenSecret;
    private String searchQueries;

 
    private String[] queryTerms; 
    private Twitter4jStatusClient t4jClient;
    private int numProcessingThreads = 3;
    private  int noOfListeners = 1;
    BasicClient client;  
    TopicMessageProducer topic;
    static final Logger log = LoggerFactory.getLogger(TwitterQueryStream.class);
    
    
        protected static int  instanceCount=0;
    private String queryName;
    
    public TwitterQueryStream(){
          super();
        instanceCount++;
      
    }
    
    @Override
    protected void finalize() throws Throwable
    {
        super.finalize();
        instanceCount--;

    } 

    public static int getInstanceCount()
    {
        return instanceCount;
    }
    
    
    public void setup(){
        configure();
         ConfigurationBuilder cb = new ConfigurationBuilder();
          cb.setDebugEnabled(true).setJSONStoreEnabled(true).setOAuthConsumerKey(oAuthConsumerKey)
                .setOAuthConsumerSecret(oAuthConsumerSecret)
                .setOAuthAccessToken(oAuthAccessToken)
                .setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
      
         connect();
    } 
    
   public String getSearchQueries() {
        return searchQueries;
    }
   


    
    public void connect(){
    
      BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
     Authentication auth = new OAuth1(oAuthConsumerKey, oAuthConsumerSecret, oAuthAccessToken, oAuthAccessTokenSecret); 
     StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
     // add some track terms
     log.trace("Setting up Stream end points for query terms: "+searchQueries);
     endpoint.trackTerms(Lists.newArrayList(queryTerms));
     
     client = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue))
      .build();
   
     // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
    // calling the listeners on each message
      log.trace("Setting up "+numProcessingThreads+" executor thread(s)");
      ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);
      log.trace("Setting up "+noOfListeners+" stream listener(s)");   
      ArrayList listeners =  Lists.newArrayList();
      for (int i=0;i<noOfListeners+1;i++){
         TwitterQueryListener queryListener = new TwitterQueryListener();
         queryListener.setQueryTerms(queryTerms);
         queryListener.setListenerQuery(searchQueries);
         queryListener.setQueryName(queryName);
         queryListener.setStartedTime(new Date());
         queryListener.setTopic(topic);
         listeners.add(queryListener );
      
      }
    // Wrap our BasicClient with the twitter4j client
       t4jClient    = new Twitter4jStatusClient( client, 
                                                 queue, 
                                                    listeners, 
                                                    service);

        // Establish a connection
        t4jClient.connect();
    }
    
    

    
    private void configure() {    
        oAuthConsumerKey = AppProperties.getProperty("ConsumerKey");
        oAuthConsumerSecret = AppProperties.getProperty("ConsumerSecret");  
        oAuthAccessToken = AppProperties.getProperty("AccessToken");
        oAuthAccessTokenSecret = AppProperties.getProperty("AccessTokenSecret");
        searchQueries = AppProperties.getProperty("QueryTerms");
        queryName = AppProperties.getProperty("QueryName");
        queryTerms = searchQueries.split("\\s*,\\s*");
        noOfListeners = Integer.parseInt(AppProperties.getProperty("listeners"));
        numProcessingThreads = Integer.parseInt(AppProperties.getProperty("threads"));  
        topic = new TopicMessageProducer();
        topic.setTopic(AppProperties.getProperty("kafka.topic"));
     
        log.trace("Stream basic properties loaded and configured from "+AppProperties.getPropertiesFile());   
        }
    
    public void run() throws InterruptedException {
        log.trace("Number of streams running "+ getInstanceCount());
        topic.start();
        for (int threads = 0; threads < numProcessingThreads; threads++) {
        // This must be called once per processing thread
      
            t4jClient.process();
        }
        log.trace("Stream listener(s) are running..");
    }

    
    
}
