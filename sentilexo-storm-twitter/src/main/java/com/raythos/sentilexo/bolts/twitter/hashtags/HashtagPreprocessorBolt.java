

package com.raythos.sentilexo.bolts.twitter.hashtags;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.bolts.BaseRichRaythosBolt;
import java.util.Map;


import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.utils.DateTimeUtils;
import java.util.Date;
import java.util.List;

/**
 *
 * @author yanni
 */

public class HashtagPreprocessorBolt extends BaseRichRaythosBolt {
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("queryName","statusId","hashtag","createdAt","retweet")); 
    }

   
    @Override
    public void execute(Tuple tuple) {
        Long statusId = (long)tuple.getValue(0);
        
        try {
                TwitterQueryResultItemAvro result = (TwitterQueryResultItemAvro) tuple.getValue(1);
                String dateStr = result.getCreatedAtAsString();

                log.trace("processing status id " + statusId + "with creation date "+ dateStr);
                Date resultCreationDate = new Date(); 
                resultCreationDate.setTime(result.getCreatedAt());
                List<String> hashtags;
                hashtags = result.getHashtags();
                for (String hashtag : hashtags) {
                                String lowerCaseHashTag  =hashtag.toLowerCase();
                                collector.emit(new Values(result.getQueryName(), statusId, 
                                                          lowerCaseHashTag,
                                                           resultCreationDate,
                                                           result.getRetweet()));
                       log.trace("StatusId "+statusId+" emmiting hashtag "+lowerCaseHashTag);         
                        }
                log.trace("hashtags for status  "+statusId+ " emmited");
                result = null;
                collector.ack(tuple);
                }
           
        catch (Exception e ) {
           log.error("error when pre-processing hashtags for statud id "+statusId+". Error msg "+e);
        }
        
        }
        
  } 

