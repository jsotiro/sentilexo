/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.Date;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class CalculateHashtagTotals  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(CalculateHashtagTotals.class);
    int counter = 0;
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String owner = tuple.getString(0);
        String query = tuple.getString(1);
        long statusId = (long)tuple.getLong(2);
        String hashtag = tuple.getStringByField("hashtag");
        Date createdAt = (Date)tuple.getValueByField("cAt");
        int retweet = (int)tuple.getIntegerByField("retwt");
        try{
            counter++;
            TwitterDataManager.getInstance().updateHashTagTotals(owner,query,hashtag,createdAt,retweet);
            log.trace("#"+counter + " Hashtag totals for StatusId = "+ statusId + " written to Cassandra keyspace"+ TwitterDataManager.getInstance().getKeyspace());
            collector.emit(new Values(counter));
        }
        catch (Exception e)     {
        log.error("error when updating totals for hashtag "+hashtag +"for statusId "+statusId +" to Cassandra. Error msg "+e);
        }
    }   
    
}
