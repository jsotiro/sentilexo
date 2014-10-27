

package com.raythos.sentilexo.bolts.twitter;

import com.raythos.sentilexo.bolts.BaseCQLBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;


import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;



/**
 *
 * @author yanni
 */

public class TwitterJSONCQLPersistenceBolt extends BaseCQLBolt {
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("status")); 
    }


    @Override
    public void execute(Tuple tuple) {
        
        String message = tuple.getString(0);
        
        String delimiter = ""+(char)9;
        String messageFields[] = message.split(delimiter);
        long statusId = Long.parseLong(messageFields[0]);
        String json = messageFields[1];
        try{
            TwitterDataManager.getInstance().saveTwitterJson(statusId, json);
            log.trace("Json for StatusId = "+ statusId + " written to Cassandra keyspace"+cqlKeyspace);
            collector.ack(tuple);
            json = null;
        }
        catch (Exception e) {
        log.error("error when saving json for statusId "+statusId+ " to Cassandra. Error msg"+e);
     
        }
        
    } 
}
