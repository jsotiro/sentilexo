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
package com.raythos.sentilexo.bolts.twitter.hashtags;

import com.raythos.sentilexo.bolts.BaseCQLBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.HashtagTotals;
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
        long statusId = (long) tuple.getValue(1);
        String hashtag = tuple.getString(2);
        Date createdAt = (Date) tuple.getValue(3);
        boolean isStatusRetweet = (Boolean) tuple.getValue(4);
        int retweet = (isStatusRetweet == true) ? 1 : 0;
        try {
            HashtagTotals dataItem = new HashtagTotals();
            dataItem.updateHashTagTotals("raythos", query, hashtag, createdAt, retweet);
            log.trace("Hashtag totals for StatusId = " + statusId + " written to Cassandra keyspace" + cqlKeyspace);
            collector.ack(tuple);
            // collector.emit(new Values(result));
        } catch (Exception e) {
            log.error("error when updating totals for hashtag " + hashtag + "for statusId " + statusId + " to Cassandra. Error msg " + e);

        }

    }
}
