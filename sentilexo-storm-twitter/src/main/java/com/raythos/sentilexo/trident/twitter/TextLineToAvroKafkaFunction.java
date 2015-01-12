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

import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.domain.QueryResultItemMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author yanni
 */
@SuppressWarnings({"serial", "rawtypes"})
public class TextLineToAvroKafkaFunction extends BaseFunction {

    protected static Logger log = LoggerFactory.getLogger(TextLineToAvroKafkaFunction.class);

    public TextLineToAvroKafkaFunction() {
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector tc) {
        String owner = tuple.getString(0);
        String queryName = tuple.getString(1);
        String queryTerms = tuple.getString(2);       
        String jsonText = tuple.getString(3);
        Status status = null;
        try {
            log.trace("processing json " + jsonText);
            if (jsonText.startsWith("{\"created_at")) {
                status = TwitterObjectFactory.createStatus(jsonText);
                log.trace("Status object was created from " + jsonText);
                TwitterQueryResultItemAvro avroObject = QueryResultItemMapper.mapItem(owner, queryName, queryTerms, status);
            /* todod 
                Post test and avro to kafka queue
            */
            } else {
                log.warn("No twitter status found in: " + jsonText);
            }
        } catch (Exception ex) {
            log.error("Exception was raised: " + ex);
        }      
    }

}
