

package com.raythos.sentilexo.bolts.twitter.hashtags;

import com.raythos.sentilexo.bolts.BaseCQLBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.Date;




/**
 *
 * @author yanni
 */

public class HashTagAggregationsBolt extends BaseCQLBolt {
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("status")); 
    }

  
    
 
    
    @Override
    public void execute(Tuple tuple) {
        
        //("queryName","statusId","hashtag","createdAt","retweet")); 
        String query = tuple.getString(0);
        long statusId = (long)tuple.getValue(1);
        String hashtag = tuple.getString(2);
        Date createdAt = (Date)tuple.getValue(3);
        boolean isStatusRetweet= (Boolean )tuple.getValue(4);
        int retweet = ( isStatusRetweet==true)?1:0;
        try{
            TwitterDataManager.getInstance().updateHashTagTotals("raythos",query,hashtag,createdAt,retweet);
            log.trace("Hashtag totals for StatusId = "+ statusId + " written to Cassandra keyspace"+cqlKeyspace);
            collector.ack(tuple);
           // collector.emit(new Values(result));
        }
        catch (Exception e)     {
        log.error("error when updating totals for hashtag "+hashtag +"for statusId "+statusId +" to Cassandra. Error msg "+e);
     
        }
        
    } 
}
