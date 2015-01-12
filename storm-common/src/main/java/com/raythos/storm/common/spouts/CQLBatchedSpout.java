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
package com.raythos.storm.common.spouts;

import java.io.IOException;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.common.utils.AppProperties;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import com.raythos.sentilexo.twitter.domain.ResultItem;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"serial", "rawtypes"})
public class CQLBatchedSpout implements IBatchSpout {

    protected static Logger log = LoggerFactory.getLogger(CQLBatchedSpout.class);
    protected CQLBatchReader reader;
    
    public CQLBatchReader getReader() {
        return reader;
    }

    public CQLBatchedSpout() throws IOException {
        this(5);
    }

    public CQLBatchedSpout(int batchSize) throws IOException {
        this.reader = new CQLBatchReader();
        this.reader.setBatchSize(batchSize);
    }

    protected void checkForNewData() {
         this.reader.startReadingData();
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        log.trace("Stream basic properties loaded and configured from " + AppProperties.getPropertiesFile());
        checkForNewData();
    }

    protected Values getNext(Row row) {
        Values result = null;
        if (row!=null) {
           Long id =  row.getLong(0);
           String owner = row.getString(1);
           String query_name = row.getString(2);
           ResultItem dataItem=new ResultItem(id);
           if  ( dataItem.load() );
                 dataItem.getFields().put(QueryResultItemFieldNames.QUERY_NAME, query_name);
           result = new Values(getReader().getTopologyName(),owner,query_name,dataItem.getFields());
        }
        return result;
    }

    Iterator<Row> iterator;
    
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
       
        // emit batchSize 
        Values result = null;
        while (iterator.hasNext()) {
            result = getNext(iterator.next());
            if (result != null) {
                collector.emit(result);
            }
        }

        if (reader.isFinished()) {
            checkForNewData();
        } else {
             reader.readNextBatch();
            }
        }



    @Override
    public void ack(long batchId) {
        // nothing to do here
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Map getComponentConfiguration() {
        // no particular configuration here
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("owner","query","id");
    }
  
}
