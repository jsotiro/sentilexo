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

import com.raythos.sentilexo.twitter.domain.Deployments;
import com.raythos.sentilexo.twitter.domain.QueryIndex;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class DuplicatesFilter extends BaseFilter {
   protected  static Logger log = LoggerFactory.getLogger(DuplicatesFilter.class);
   private String topologyName;
   int totals=0;
   public DuplicatesFilter(String topologyName) {
       this.topologyName = topologyName;
  }

  @Override

  public boolean isKeep(TridentTuple tuple) {
     Map item =  (Map)tuple.get(0);
     String ownerName = (String)item.get(QueryResultItemFieldNames.QUERY_OWNER);
     String queryName = (String)item.get(QueryResultItemFieldNames.QUERY_NAME);
     long id = (long)item.get(QueryResultItemFieldNames.STATUS_ID);
      
    /* when we get serialiser registered and working with Kryo we will revert to this
      TwitterQueryResultItemAvro item = (TwitterQueryResultItemAvro)tuple.get(0);
     For now use a lightweight Map that Storm can handle.
     from https://storm.apache.org/documentation/Serialization.html
     "By default, Storm can serialize primitive types, strings, byte arrays, ArrayList, HashMap, HashSet, 
     and the Clojure collection types. If you want to use another type in your tuples, 
     you'll need to register a custom serializer."
     
     
    */
    /*
    String ownerName = item.getQueryOwner();
    String queryName = item.getQueryName();
    long id = item.getStatusId();
    */
      
     boolean duplicate = QueryIndex.exists(ownerName, queryName, id);
     log.trace("Duplicate "+duplicate +" Item "+ id + "for "+ownerName+","+queryName);
     
     if (!duplicate) {
           log.info("Updating query indices for Item with StatusId = "+ id); 
           QueryIndex dataItem = new QueryIndex(ownerName, queryName,id, Deployments.getInstance(topologyName).getDeploymentNo());
           dataItem.save();
     }
     totals++;
      log.warn(totals + "items for Duplicates Filter. Last item with duplicate status "+ duplicate +" was item "+id);     
  
       
     return !duplicate;                 
  }

    
}
