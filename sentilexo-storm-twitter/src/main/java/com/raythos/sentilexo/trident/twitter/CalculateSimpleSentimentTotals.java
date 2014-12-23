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


package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.domain.StatusFieldNames;
import com.raythos.sentilexo.persistence.cql.DataManager;
import java.util.Date;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class CalculateSimpleSentimentTotals  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(CalculateSimpleSentimentTotals.class);
    

      public static final Fields hashtagFields = new Fields("qOwner.1", "qName.1", "sId.1", "createdAt.1","retweet.1", "hashtags");
    
    int counter = 0;
    List positiveKeywords;
    List negativeKeywords;

   
    
    public CalculateSimpleSentimentTotals(List positiveKeywords, List negativeKeywords){
     super();
     this.positiveKeywords = positiveKeywords;
     this.negativeKeywords = negativeKeywords;
    
    }
    
    
    
    private boolean anyHashtagStartsWithKeyword(List<String> hashtags, String keyword) {
        boolean result = false;
        keyword = keyword.replaceFirst("%", "");
        for (String h : hashtags) {
           result = h.startsWith(keyword);
           if (result) break;
        }
        return result;
    }
    
    

    private boolean anyHashtagEndsWithKeyword(List<String> hashtags, String keyword) {
        boolean result = false;
        try {
        String adjKeyword = keyword.substring(0, keyword.length()-1);
        for (String h : hashtags) {
           result = h.endsWith(adjKeyword);
           if (result) break;
        }
        }
        catch (Exception e) {
            log.error("Error whilst checking hashtags if finish with keyword " + keyword + ". Error Msg: "+e.getMessage());
            
        }
        return result;
    }

    
    
    
    public boolean hashtagListContainsAnyKeyword(List<String> hashtags, List<String> keywords) {
        boolean result = false;
        for (String s : keywords){
          if (s.startsWith("%")) 
              result = anyHashtagStartsWithKeyword(hashtags, s);
           else
              if (s.endsWith("%")) {
                result = anyHashtagEndsWithKeyword(hashtags, s);
              }
              else result = hashtags.contains(s.toLowerCase());
            if (result) break; 
        }
        return result;
    }
    
    
    public String calcSentiment(List<String> hashtags) {
      String  result = Sentiments.UNCLEAR_SENTIMENT;
      if ( hashtagListContainsAnyKeyword(hashtags, positiveKeywords) )
         result  = Sentiments.POSITIVE_SENTIMENT;
      else  if ( hashtagListContainsAnyKeyword(hashtags, negativeKeywords) )
         result  = Sentiments.NEGATIVE_SENTIMENT;     
      return result;
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
            List<String> hashtags  =  (List<String>)result.get(StatusFieldNames.HASHTAGS);
            boolean isStatusRetweet=  (boolean)result.get(StatusFieldNames.RETWEET);
            int retweet = ( isStatusRetweet==true)?1:0;
            String sentiment = calcSentiment(hashtags);
            DataManager.getInstance().updateSentimentTotals("simple", qOwner,qName,sentiment,resultCreationDate,retweet);  
            log.trace("Hashtag totals for StatusId = "+sId + " written to Cassandra keyspace"+ DataManager.getInstance().getKeyspace());
            // query, 
            // statusId, 
            collector.emit(new Values(  qOwner,
                                        qName,
                                        sId,
                                        resultCreationDate,
                                        retweet,
                                        hashtags));
          log.trace("StatusId "+sId+" emiting hashtags");         
          result = null; 
          }
        catch (Exception e)     {
            log.error("error when calculating simple sentiment totals for status for statusId "+sId +". Error msg "+e);
        }
    }   

}
