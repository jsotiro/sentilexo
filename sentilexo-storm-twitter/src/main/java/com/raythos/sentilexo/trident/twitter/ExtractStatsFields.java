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
   
     public static final Fields statsFields = new Fields("statusId","createdAt");
  
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        
        Long statusId = (long)tuple.getLongByField("StatusId");
        try {
            Map<String,Object> result = (Map) tuple.getValueByField("ResultItem");
        
            Date resultCreationDate = new Date(); 
            resultCreationDate.setTime(((Date)result.get(QueryResultItemFieldNames.CREATED_AT)).getTime());
            List<Object> v = new ArrayList<>(); 
        //     v.add("raythos"); // will param in the next iteration - for now only one owner 
            // v.add(query); 
             v.add(statusId); 
             v.add(resultCreationDate.getTime()); 
                    
                    
          collector.emit(v);
          log.trace("StatusId "+statusId+" emiting stats fields to aggregator");         
          //result = null; 
          }
        catch (Exception e)     {
            log.error("error when emiting stats fields to aggregator for statusId "+statusId +". Error msg "+e);
        }
    }   

}
