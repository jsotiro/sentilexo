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
package com.raythos.sentilexo.spouts;

import java.io.IOException;
import java.text.ParseException;
import backtype.storm.tuple.Values;
import static com.raythos.sentilexo.spouts.TextFileReaderWorker.log;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.QueryResultItemMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 * A Spout that emits fake tweets from json files. useful in testing topologies
 * without having to go through the whole Kafka round trip
 *
 * @author yanni
 */
@SuppressWarnings({"serial", "rawtypes"})
public class JSONFileTwitterSpout extends  TextFileBatchedLinesSpout {

    protected static Logger log = LoggerFactory.getLogger(JSONFileTwitterSpout.class);
    private int statusesRead;

    public JSONFileTwitterSpout() throws IOException {
        super();
    }

    public JSONFileTwitterSpout(int batchSize) throws IOException {
        super(batchSize);
        getFileWorker().setFileExt(".json");
    }

    public int getStatusesRead() {
        return statusesRead;
    }

    public void setStatusesRead(int statusesRead) {
        this.statusesRead = statusesRead;
    }
    
    @Override 
    protected Values getNextLine() {
        Values result = null;
        if (worker.getBuffer().size() > 0) {
            String lineToProcess = (String) worker.getBuffer().get(0);
            Status status = getStatusFromRawJsonLine(lineToProcess);
            if (status != null) {
                byte[] data = getSerialisedStatusObject(status);
                result = new Values(data);

            }
        }
        return result;
    }
protected Status getStatusFromRawJsonLine(String rawJSONLine) {
        int lineNo = this.getFileWorker().getLinesRead();
        String filename  = this.getFileWorker().getFilename();
        Status status = null;
        try {
            log.trace("processing File + " + this.getFileWorker().getFilename() + " - line #" + lineNo);
            if (rawJSONLine.startsWith("{\"created_at")) {
                status = TwitterObjectFactory.createStatus(rawJSONLine);
                statusesRead++;
                log.trace("File + " + filename + "line #" + lineNo + "containes twitter status. So far " + statusesRead + " Status objects read");

            } else {
                log.warn("File " + filename + " line " + lineNo + " has no twitter status JSON text");
            }
        } catch (Exception ex) {
            log.error("Exception was raised: " + ex);
        }
        return status;
    }
    

    @Override
     void scanPathForFileReads() {
       statusesRead=0;  
       super.scanPathForFileReads();
    }

    byte[] getSerialisedStatusObject(Status status) {
        byte[] data = null;
        log.trace("Posting Status with id " + status.getId() + " from File " + this.worker.getFilename());
        try {
            TwitterQueryResultItemAvro tqri = new TwitterQueryResultItemAvro();
            tqri = QueryResultItemMapper.mapItem(this.worker.getQueryName(), this.worker.getQueryTerms(), status);
            data = QueryResultItemMapper.getAvroSerialized(tqri);
            log.trace("AVRO serialised Status with id " + status.getId() + " was obtained from file" + this.worker.getFilename());
        } catch (Exception e) {
            log.error("Error when emmitting bytes. The exception was " + e);
        }
        return data;
    }

    

}
