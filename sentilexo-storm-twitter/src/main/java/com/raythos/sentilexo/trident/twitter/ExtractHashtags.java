/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
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
        String query = tuple.getString(0);
        Long statusId = (long)tuple.getValue(1);
        try{
            Date resultCreationDate = (Date)tuple.getValue(2);
            Integer retweet = (int)tuple.getValue(3);
            List<String> hashtags = (List)tuple.getValue(4);
            log.trace("processing status id " + statusId + "with creation date "+ resultCreationDate);
            for (String hashtag : hashtags) {
                String lowerCaseHashTag  =hashtag.toLowerCase();
                collector.emit(new Values( query, 
                                           statusId, 
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
