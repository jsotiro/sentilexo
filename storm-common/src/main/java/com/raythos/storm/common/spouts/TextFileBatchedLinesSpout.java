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
import com.raythos.sentilexo.common.utils.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spout that emits lines from text files in batches and processes them.
 * directly instead of doing the Kafka round trip.
 * useful esp for testing topologies with small files during development
 * @author yanni
 */
@SuppressWarnings({"serial", "rawtypes"})
public class TextFileBatchedLinesSpout implements IBatchSpout {

    protected static Logger log = LoggerFactory.getLogger(TextFileBatchedLinesSpout.class);
    protected TextFileReaderWorker worker;

    public TextFileReaderWorker getFileWorker() {
        return worker;
    }

    public TextFileBatchedLinesSpout() throws IOException {
        this(5);
    }

    public TextFileBatchedLinesSpout(int batchSize) throws IOException {
        this.worker = new TextFileReaderWorker();
        this.worker.setBatchSize(batchSize);
        this.worker.setBufferSize(batchSize * 1000);
    }

    protected void scanPathForFileReads() {
        int filestoProcess = this.worker.scanForFilesFromPath();
        log.trace(filestoProcess + " files scanned for processing");
        try {
            if (filestoProcess > 0) {
                this.worker.startReadingFiles();
            }
        } catch (IOException ex) {
            log.error("Error when starting reading files. The exception was ", ex);
        }
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        // init
        this.worker.setQueryName(AppProperties.getProperty("QueryName"));
        this.worker.setQueryTerms(AppProperties.getProperty("QueryTerms"));
        log.trace("Stream basic properties loaded and configured from " + AppProperties.getPropertiesFile());
        scanPathForFileReads();
    }

    protected Values getNextLine() {
        Values result = null;
        if (worker.getBuffer().size() > 0) {
            String data = (String) worker.getBuffer().get(0);
                result = new Values( worker.getQueryOwner(), 
                                     worker.getQueryName(),
                                     worker.getQueryTerms(),
                                     data);
        }
        return result;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        // emit batchSize 
        Values result = null;
        for (int i = 0; i < this.worker.getBuffer().size(); i++) {
            result = getNextLine();
            worker.getBuffer().remove(0);
            if (result != null) {          
                collector.emit(result);
              
            }
        }

        if (worker.isFinished()) {
            scanPathForFileReads();
        } else {
            try {
                worker.readNextBacthOfFileLines();
            } catch (IOException ex) {
                log.error("error reading next batch. The exception was ", ex);
            }
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
        return new Fields("_queryOwner","_queryName", "_queryTerms","line");
    }
  
}
