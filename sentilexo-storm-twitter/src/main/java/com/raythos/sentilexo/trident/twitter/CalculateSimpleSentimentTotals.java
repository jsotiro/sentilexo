/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.common.domain.StatusFieldNames;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
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
    
    public static final Fields hashtagsFields = new Fields("qOwner", "qName", "sId", "createdAt","retweet", "hashtags");

    
    int counter = 0;
    List positiveKeywords;
    List negativeKeywords;
    static final String  POSITIVE_SENTIMENT = "YES";
    static final String  NEGATIVE_SENTIMENT = "NO";
    static final String  UNCLEAR_SENTIMENT = "UNCLEAR";
   
    
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
      String  result = UNCLEAR_SENTIMENT;
      if ( hashtagListContainsAnyKeyword(hashtags, positiveKeywords) )
         result  = POSITIVE_SENTIMENT;
      else  if ( hashtagListContainsAnyKeyword(hashtags, negativeKeywords) )
         result  = NEGATIVE_SENTIMENT;     
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
            TwitterDataManager.getInstance().updateSimpleSentimentTotals(qOwner,qName,sentiment,resultCreationDate,retweet);  
            log.trace("Hashtag totals for StatusId = "+sId + " written to Cassandra keyspace"+ TwitterDataManager.getInstance().getKeyspace());
            // query, 
            // statusId, 
            collector.emit(new Values(  qOwner,qName,sId,
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
