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


package com.raythos.sentilexo.trident;

import com.raythos.sentilexo.twitter.domain.DeployedTopology;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class UpdateTopologiesJournal extends BaseFilter {
   protected  static Logger log = LoggerFactory.getLogger(UpdateTopologiesJournal.class);
   private String topologyName;
   int totals =0;
   
   
   public UpdateTopologiesJournal(String topologyName) {
       this.topologyName = topologyName;
  }

  @Override

  public boolean isKeep(TridentTuple tuple) {
     Map item =  (Map)tuple.get(0);

     String queryOwner = (String)item.get(QueryResultItemFieldNames.QUERY_OWNER);
     String queryName = (String)item.get(QueryResultItemFieldNames.QUERY_NAME);
     long statusId = (long)item.get(QueryResultItemFieldNames.STATUS_ID);       
      
     
     /*TwitterQueryResultItemAvro item = (TwitterQueryResultItemAvro) tuple.get(0);
     String queryOwner = item.getQueryOwner();
     String queryName = item.getQueryName();
     long statusId = item.getStatusId();
   */
     DeployedTopology dataItem = new DeployedTopology(topologyName, queryOwner,queryName, statusId, new Date());
     dataItem.save();   
     totals++; 
     log.warn(totals + "items for Topologies journal updated. Last item was for "+ topologyName +" and item "+statusId);     
     return true;                
  }

    
}
