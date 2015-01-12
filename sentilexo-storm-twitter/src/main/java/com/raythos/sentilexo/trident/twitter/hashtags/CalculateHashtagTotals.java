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

package com.raythos.sentilexo.trident.twitter.hashtags;

import backtype.storm.tuple.Values;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.HashtagTotals;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import java.util.Date;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class CalculateHashtagTotals  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(CalculateHashtagTotals.class);
    int counter = 0;
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String owner = tuple.getStringByField(QueryResultItemFieldNames.QUERY_OWNER);
        String query = tuple.getStringByField(QueryResultItemFieldNames.QUERY_NAME);
        Date createdAt = (Date)tuple.getValueByField(QueryResultItemFieldNames.CREATED_AT);
        long statusId = tuple.getLongByField(QueryResultItemFieldNames.STATUS_ID);
        String hashtag = tuple.getStringByField("hashtag");

        int retweet = (int)tuple.getIntegerByField(QueryResultItemFieldNames.RETWEET);
        try{
            counter++;
            HashtagTotals dataItem = new HashtagTotals();
            dataItem.updateHashTagTotals(owner,query,hashtag,createdAt,retweet);
            log.trace("#"+counter + " Hashtag totals for StatusId = "+ statusId + " written to Cassandra keyspace"+ DataManager.getInstance().getKeyspace());
            collector.emit(new Values(counter));
        }
        catch (Exception e)     {
        log.error("error when updating totals for hashtag "+hashtag +"for statusId "+statusId +" to Cassandra. Error msg "+e);
        }
    }   
    
}
