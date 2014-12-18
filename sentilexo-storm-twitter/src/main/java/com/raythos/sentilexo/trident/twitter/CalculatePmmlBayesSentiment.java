
package com.raythos.sentilexo.trident.twitter;
/**
 *
 * @author yanni
 */



import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;

import com.raythos.sentilexo.twitter.common.domain.StatusFieldNames;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

 
public class CalculatePmmlBayesSentiment extends BaseFunction { 

    public static final Fields statusFields = new Fields("qOwner1", "qName1", "sId1", "createdAt1","retweet1", "text1");
    protected  static Logger log = LoggerFactory.getLogger(CalculatePmmlBayesSentiment.class);
    private NaiveBayesHandler handler;
    private final String[] sentimentLabels={"YES","NO","UKNOWN"};
    private final Map<String, Float> prior;
    private final Map<String, Float> prob_map;
    private final List<String> predictors;
    private final Set<String> possibleTargets;
    
    public CalculatePmmlBayesSentiment(NaiveBayesHandler handler){
      super();
      this.handler = handler;
      prior = NaiveBayesHandler.prior;
      prob_map = NaiveBayesHandler.prob_map;
      predictors = NaiveBayesHandler.predictors;
      possibleTargets = NaiveBayesHandler.possibleTargets;
    }
       
    private String calcSentimentForTweetMessage(String statusText) {
        int sentiment;
        String prediction = handler.predictItNow(statusText, prior, predictors, prob_map, possibleTargets);
        sentiment = Integer.parseInt(prediction);
        if (sentiment > 3) {
            log.warn("Sentiment value "+sentiment +" greater than upper bound of 3. Assuming Unknown");
            sentiment = 3;
        }
        if (sentiment < 1) {
            log.warn("Sentiment value "+sentiment +" lower than lower bound of 1. Assuming Unknown");
            sentiment = 3;
        }
        
        return sentimentLabels[sentiment-1];
   }     

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String qOwner = tuple.getStringByField("owner"); 
        String qName = tuple.getStringByField("queryName");
        Long sId = (long)tuple.getLongByField("StatusId");
        try {
            Map<String,Object> result = ( Map<String,Object>) tuple.getValueByField("ResultItem");
            Date resultCreationDate = new Date(); 
            resultCreationDate.setTime(((Date)result.get(StatusFieldNames.CREATED_AT)).getTime());
            String tweetMessage = (String)result.get(StatusFieldNames.TEXT); 
            boolean isStatusRetweet=  (boolean)result.get(StatusFieldNames.RETWEET);
            int retweet = ( isStatusRetweet==true)?1:0;
            String sentiment = calcSentimentForTweetMessage(tweetMessage);
            TwitterDataManager.getInstance().updateSentimentTotals("bayes",qOwner,qName,sentiment,resultCreationDate,retweet);  
            log.trace("Bayes Sentiment for StatusId = "+sId + " written to Cassandra keyspace"+ TwitterDataManager.getInstance().getKeyspace());
            // query, 
            // statusId, 
            collector.emit(new Values(  qOwner,qName,sId,
                                        resultCreationDate,
                                        retweet,
                                        tweetMessage));
          log.trace("StatusId "+sId+" emiting status");         
          result = null; 
          }
        catch (Exception e)     {
            log.error("error when calculating bayes sentiment totals for status for statusId "+sId +". Error msg "+e);
        }
    }


}
