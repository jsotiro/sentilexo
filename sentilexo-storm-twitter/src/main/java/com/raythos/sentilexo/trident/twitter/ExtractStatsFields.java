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

import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class ExtractStatsFields  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(ExtractStatsFields.class);
    int totals = 0;
    
    public static final Fields statsFields = new Fields( QueryResultItemFieldNames.QUERY_OWNER, 
                                                         QueryResultItemFieldNames.QUERY_NAME,
                                                         QueryResultItemFieldNames.STATUS_ID,
                                                         QueryResultItemFieldNames.CREATED_AT);
  
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        
        
        Map item = (Map) tuple.getValueByField("ResultItem");        
        String queryOwner = (String)item.get(QueryResultItemFieldNames.QUERY_OWNER);
        String queryName  = (String)item.get(QueryResultItemFieldNames.QUERY_NAME);
        Long statusId = (Long)item.get(QueryResultItemFieldNames.STATUS_ID);
        
/*        TwitterQueryResultItemAvro item = (TwitterQueryResultItemAvro) tuple.getValueByField("item");        
        String queryOwner =item.getQueryOwner();
        String queryName  = item.getQueryName();
        Long statusId = item.getStatusId(); */
        
        try {
           Date resultCreationDate = (Date)item.get(QueryResultItemFieldNames.CREATED_AT);
           // Date resultCreationDate = new Date(); 
           // resultCreationDate.setTime(item.getCreatedAt());
            List<Object> v = new ArrayList<>();
             v.add(queryOwner);
             v.add(queryName);
             v.add(statusId); 
             v.add(resultCreationDate.getTime());
             
             totals++;
          log.warn(totals + " item processed by ExtractStatsFields. Last item Status Id:"+statusId);
          collector.emit(v);
          //log.warn("StatusId "+statusId+" emitted stats fields to aggregator");         
          //result = null; 
          
          
          }
        catch (Exception e)     {
            log.error("error when emiting stats fields to aggregator for statusId "+statusId +". Error msg "+e);
        }
    }   

}
