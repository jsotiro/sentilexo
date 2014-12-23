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

import backtype.storm.tuple.Values;
import java.util.Date;
import java.util.List;
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
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String owner = tuple.getStringByField("qOwner.1");
        String query = tuple.getStringByField("qName.1");
        Long statusId = (long)tuple.getLongByField("sId.1");
        try{
            Date resultCreationDate = (Date)tuple.getValueByField("createdAt.1");
            Integer retweet = (int)tuple.getIntegerByField("retweet.1");
            List<String> hashtags = (List)tuple.getValueByField("hashtags");
            log.trace("processing status id " + statusId + "with creation date "+ resultCreationDate);
           
            //query, 
            // statusId,
            for (String hashtag : hashtags) {
                String lowerCaseHashTag  =hashtag.toLowerCase();
                collector.emit(new Values( owner, query, statusId,  
                                           lowerCaseHashTag,
                                           resultCreationDate,
                                           retweet));
                log.trace("StatusId "+statusId+" emiting hashtag "+lowerCaseHashTag);         
                }
            log.trace("hashtags for status  "+statusId+ " emmited");
        }
           
        catch (Exception e ) {
           log.error("error when pre-processing hashtags for statud id "+statusId+". Error msg "+e);
        }
       
    }
    
}
