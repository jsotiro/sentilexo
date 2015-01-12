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


package com.raythos.sentilexo.trident.twitter.hashtags;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
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
public class ExtractHashtags  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(ExtractHashtags.class);
    
    public static final Fields hashtagTotalsFields = new Fields(QueryResultItemFieldNames.QUERY_OWNER, 
                                                                QueryResultItemFieldNames.QUERY_NAME,
                                                                QueryResultItemFieldNames.STATUS_ID,
                                                                QueryResultItemFieldNames.CREATED_AT,
                                                                QueryResultItemFieldNames.RETWEET,
                                                                "hashtag"
            
    );
    
        @Override  
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Map item = (Map) tuple.getValueByField("ResultItem");        
        String queryOwner = (String)item.get(QueryResultItemFieldNames.QUERY_OWNER);
        String queryName  = (String)item.get(QueryResultItemFieldNames.QUERY_NAME);
        Long statusId = (Long)item.get(QueryResultItemFieldNames.STATUS_ID);
        try{
            Date resultCreationDate = new Date();
            resultCreationDate.setTime((Long)item.get(QueryResultItemFieldNames.CREATED_AT));
            boolean isStatusRetweet=  (boolean)item.get(QueryResultItemFieldNames.RETWEET);
            int retweet = ( isStatusRetweet==true)?1:0;
            List<String> hashtags = (List)item.get(QueryResultItemFieldNames.HASHTAGS);
     
            log.trace("processing status id " + statusId + "with creation date "+ resultCreationDate);
           
            //query, 
            // statusId,
            for (String hashtag : hashtags) {
                String lowerCaseHashTag  =hashtag.toLowerCase();
                collector.emit(new Values( queryOwner, 
                                           queryName, 
                                           statusId,  
                                           resultCreationDate,
                                           retweet,
                                           lowerCaseHashTag));
                log.trace("StatusId "+statusId+" emiting hashtag "+lowerCaseHashTag);         
                }
            log.trace("hashtags for status  "+statusId+ " emmited");
        }
           
        catch (Exception e ) {
           log.error("error when pre-processing hashtags for statud id "+statusId+". Error msg "+e);
        }
       
    }
    
}
