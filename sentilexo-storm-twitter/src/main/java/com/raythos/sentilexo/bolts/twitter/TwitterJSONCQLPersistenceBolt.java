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

package com.raythos.sentilexo.bolts.twitter;

import com.raythos.sentilexo.bolts.BaseCQLBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.raythos.sentilexo.twitter.domain.ResultJson;



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
            
            ResultJson dataItem = new ResultJson(statusId, json);
            dataItem.save();
            log.trace("Json for StatusId = "+ statusId + " written to Cassandra keyspace"+cqlKeyspace);
            collector.ack(tuple);
            json = null;
        }
        catch (Exception e) {
        log.error("error when saving json for statusId "+statusId+ " to Cassandra. Error msg"+e);
     
        }
        
    } 
}
