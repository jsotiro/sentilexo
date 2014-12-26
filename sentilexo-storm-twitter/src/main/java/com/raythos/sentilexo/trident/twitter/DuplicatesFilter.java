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
   public DuplicatesFilter(String topologyName) {
       this.topologyName = topologyName;
  }

  @Override

  public boolean isKeep(TridentTuple tuple) {
     String owner = tuple.getStringByField("owner");
     String queryName = tuple.getStringByField("queryName");
     long id = tuple.getLongByField("StatusId");
     boolean duplicate = QueryIndex.exists(owner, queryName, id);
     log.info("Duplicate "+duplicate +" Item "+ id + "for "+owner+","+queryName);
     if (!duplicate) {
           QueryIndex dataItem = new QueryIndex(owner, queryName,id, Deployments.getInstance(topologyName).getDeploymentNo());
           dataItem.save();
     }
     
     return !duplicate;                 
  }

    
}
