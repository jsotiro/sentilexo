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
import backtype.storm.tuple.Values;


import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.TwitterQueryResultItemMapper;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.TwitterResultItem;
import java.util.Map;




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
        TwitterQueryResultItemAvro object = (TwitterQueryResultItemAvro) tuple.getValue(1);
       // try{
            TwitterResultItem dataItem = new TwitterResultItem(object);
            dataItem.save();
            log.warn("Result Item StatusId = "+ statusId + " written to Cassandra keyspace"+cqlKeyspace);
            collector.emit(new Values(statusId,dataItem.getFields()));
            collector.ack(tuple);
       // }
       // catch (Exception e) {
         //  log.error("error when saving result for statusId "+statusId+ " to Cassandra. Error msg "+e);
  
     
       // }
        
    } 
}
