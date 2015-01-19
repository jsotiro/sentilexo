/*
 * Copyright 2015 (c) Raythos Interactive Ltd. - http://www.raythos.com
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
package com.raythos.sentilexo.topologies.twitter;

import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.trident.twitter.hashtags.CalculateHashtagTotals;
import com.raythos.sentilexo.trident.twitter.hashtags.CalculateSimpleSentimentTotals;
import com.raythos.sentilexo.trident.twitter.hashtags.ExtractHashtags;
import com.raythos.sentilexo.twitter.domain.DefaultSetting;
import storm.trident.Stream;

/**
 *
 * @author John Sotiropoulos
 */
public class HashtagAnalysisTopology extends BaseStreamAnalyticsTopology {
    
     private static final String POSITIVE_KEYWORDS="positivekeywords";
     private static final String NEGATIVE_KEYWORDS="negativekeywords";
     private static final Fields hashtagTotalFields = ExtractHashtags.hashtagTotalsFields;
     private static final Fields counterField = new Fields("count");
     
    @Override
    public void defineFunctions() {
        // keyword-based ("bag of words") sentiment classification
        DefaultSetting positiveKeywords = new DefaultSetting(POSITIVE_KEYWORDS, null);
        positiveKeywords.load();
        DefaultSetting negativeKeywords = new DefaultSetting(NEGATIVE_KEYWORDS, null);
        negativeKeywords.load();
        CalculateSimpleSentimentTotals simpleSentimentFunction = new CalculateSimpleSentimentTotals(positiveKeywords.getValues(), negativeKeywords.getValues());
        Fields hashtagFields = CalculateSimpleSentimentTotals.hashtagFields;
        
        Stream analysisStream = 
                getMainStream()        
                .each(itemField, simpleSentimentFunction,hashtagFields )
                .each(new Fields(itemField.get(0),hashtagFields.get(0)), new ExtractHashtags(), hashtagTotalFields)
                .each(hashtagTotalFields, new CalculateHashtagTotals(),counterField);
     
    }    

     
     
     public static void main(String[] args)  {
        HashtagAnalysisTopology hashtag_topology = new  HashtagAnalysisTopology();
        hashtag_topology.configFromArgs(args);
        hashtag_topology.execute();        
    } 
    
}
