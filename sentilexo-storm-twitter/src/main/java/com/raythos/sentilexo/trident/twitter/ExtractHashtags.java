/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
        String owner = tuple.getStringByField("qOwner");
        String query = tuple.getStringByField("qName");
        Long statusId = (long)tuple.getLongByField("sId");
        try{
            Date resultCreationDate = (Date)tuple.getValueByField("createdAt");
            Integer retweet = (int)tuple.getIntegerByField("retweet");
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
