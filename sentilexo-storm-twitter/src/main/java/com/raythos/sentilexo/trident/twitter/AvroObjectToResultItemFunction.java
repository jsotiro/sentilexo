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

package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.ResultItem;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 *
 * @author yanni
 */
public class AvroObjectToResultItemFunction  extends BaseFunction {
    
   protected static org.slf4j.Logger   log = LoggerFactory.getLogger(AvroObjectToResultItemFunction.class);
        
   @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
             
             TwitterQueryResultItemAvro avroObject = (TwitterQueryResultItemAvro)tuple.get(0);
             String queryName = avroObject.getQueryName();
             if (queryName==null || queryName.equals(""))
                 avroObject.setQueryName("indyref");
             
            String queryOwner = avroObject.getQueryOwner();
             if (queryOwner==null || queryOwner.equals(""))
                 avroObject.setQueryOwner("raythos");

             ResultItem  dataItem = new ResultItem(avroObject);
             dataItem.save();
             String clKeyspace = DataManager.getInstance().getKeyspace();
             log.info("Result Item StatusId = "+ avroObject.getStatusId() + " written to Cassandra keyspace"+clKeyspace);
             collector.emit(new Values(dataItem));
    }   
}
