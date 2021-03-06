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

package com.raythos.sentilexo.trident.twitter.sentiment;
/**
 *
 * @author yanni
 */



import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;

import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.SentimentTotals;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

 
public class DirectCalculatePmmlBayesSentiment extends BaseFunction { 
    protected  static Logger log = LoggerFactory.getLogger(DirectCalculatePmmlBayesSentiment.class);
    private NaiveBayesHandler handler;
    private final String[] sentimentLabels={"YES","NO","UKNOWN"};
    private final Map<String, Float> prior;
    private final Map<String, Float> prob_map;
    private final List<String> predictors;
    private final Set<String> possibleTargets;
    
    public DirectCalculatePmmlBayesSentiment(NaiveBayesHandler handler){
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
        Map<String,Object> item = ( Map<String,Object>) tuple.getValueByField("ResultItem");   
        String qOwner = (String)item.get(QueryResultItemFieldNames.QUERY_OWNER); 
        String qName = (String)item.get(QueryResultItemFieldNames.QUERY_NAME);
        Long sId = (Long)item.get(QueryResultItemFieldNames.STATUS_ID);
        try {
     
            Date resultCreationDate = new Date(); 
            resultCreationDate.setTime(((Long)item.get(QueryResultItemFieldNames.CREATED_AT)));
            String tweetMessage = (String)item.get(QueryResultItemFieldNames.TEXT); 
            boolean isStatusRetweet=  (boolean)item.get(QueryResultItemFieldNames.RETWEET);
            int retweet = ( isStatusRetweet==true)?1:0;
            String sentiment = calcSentimentForTweetMessage(tweetMessage);
            SentimentTotals dataItem = new SentimentTotals();
            dataItem.updateSentimentTotals("bayes",qOwner,qName,sentiment,resultCreationDate,retweet);  
            log.trace("Bayes Sentiment for StatusId = "+sId + " written to Cassandra keyspace"+ DataManager.getInstance().getKeyspace());
            // query, 
            // statusId, 
            collector.emit(new Values(sentiment));
          log.trace("StatusId "+sId+" emiting status");         
          item = null; 
          }
        catch (Exception e)     {
            log.error("error when calculating bayes sentiment totals for status for statusId "+sId +". Error msg "+e);
        }
    }


}
