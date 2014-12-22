
package com.raythos.sentilexo.trident.twitter;
/**
 *
 * @author yanni
 */



import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.raythos.sentilexo.twitter.common.domain.StatusFieldNames;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

 
public class CalculateNLPSentiment extends BaseFunction { 

    public static final Fields statusFields = new Fields("qOwner", "qName", "sId", "createdAt","retweet", "text");
    protected  static Logger log = LoggerFactory.getLogger(CalculateNLPSentiment.class);
   
    
   

    private String textSentiment(int sentiment) {
    if (sentiment<2) return Sentiments.NEGATIVE_SENTIMENT;
        else if (sentiment>3) return Sentiments.POSITIVE_SENTIMENT; 
            else return Sentiments.NEGATIVE_SENTIMENT;
    
    }
    
    private String calcSentimentForTweetMessage(String statusText) {
     
        int mainSentiment = 0;
         StanfordCoreNLP pipeline = CoreNLPSentimentClassifier.getInstance().getPipeline();
   
        if (statusText != null && statusText.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(statusText);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
 
            }
        }
        return textSentiment(mainSentiment);
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
            TwitterDataManager.getInstance().updateSentimentTotals("nlp",qOwner,qName,sentiment,resultCreationDate,retweet);  
            log.trace("Sentiment for StatusId = "+sId + " written to Cassandra keyspace"+ TwitterDataManager.getInstance().getKeyspace());
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
            log.error("error when calculating NLP sentiment totals for status for statusId "+sId +". Error msg "+e);
        }
    }


}
