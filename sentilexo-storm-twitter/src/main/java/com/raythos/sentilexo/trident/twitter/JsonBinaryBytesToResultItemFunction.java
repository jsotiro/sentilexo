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
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import com.raythos.sentilexo.twitter.domain.ResultItem;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 *
 * @author yanni
 */
public class JsonBinaryBytesToResultItemFunction  extends BaseFunction {
    
     protected static org.slf4j.Logger   log = LoggerFactory.getLogger(JsonBinaryBytesToResultItemFunction.class);
     
     
        
   @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
             byte[] bytes= tuple.getBinary(0);
             ResultItem dataItem = (ResultItem)ResultItem.fromBinaryJSon(bytes, ResultItem.class);
             log.trace("Result Item StatusId = "+ dataItem.get(QueryResultItemFieldNames.STATUS_ID) + " de-serialised from bytes");
             collector.emit(new Values(dataItem.getFields()));
    }   
}
