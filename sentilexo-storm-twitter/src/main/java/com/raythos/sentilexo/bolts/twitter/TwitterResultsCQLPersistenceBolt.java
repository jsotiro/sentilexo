

package com.raythos.sentilexo.bolts.twitter;

import com.raythos.sentilexo.bolts.BaseCQLBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;




/**
 *
 * @author yanni
 */

public class TwitterResultsCQLPersistenceBolt extends BaseCQLBolt {
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("statusId","resultItem")); 
    }


    @Override
    public void execute(Tuple tuple) {
        long statusId = (long)tuple.getValue(0);
        TwitterQueryResultItemAvro result = (TwitterQueryResultItemAvro) tuple.getValue(1);
       // try{
            TwitterDataManager.getInstance().saveTwitterQueryResultItem(result);
            log.warn("Result Item StatusId = "+ statusId + " written to Cassandra keyspace"+cqlKeyspace);
            collector.emit(new Values(statusId,result));
            collector.ack(tuple);
       // }
       // catch (Exception e) {
         //  log.error("error when saving result for statusId "+statusId+ " to Cassandra. Error msg "+e);
  
     
       // }
        
    } 
}
