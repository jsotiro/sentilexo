/*
 * Copyright 2014 (c) Raythos Interactive Ltd. - http://www.raythos.com
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
package com.raythos.sentilexo.topologies.twitter;

import com.raythos.sentilexo.common.utils.AppProperties;
import com.raythos.sentilexo.topologies.base.CoreTopology;
import com.raythos.sentilexo.trident.twitter.JsonBinaryBytesToResultItemFunction;
import com.raythos.sentilexo.trident.twitter.TextLineToResultItemFunction;

/**
 *
 * @author John Sotiropoulos
 */


public class BaseStreamAnalyticsTopology extends CoreTopology {
     
   
    @Override
    public void defineFunctions() {
    }    
    
    @Override
    public void initDeserializerFunctions() {
        this.setBinaryDeserializerFunction(new JsonBinaryBytesToResultItemFunction());
        this.setTextDeserializerFunction(new TextLineToResultItemFunction());         
    }
    
    

  @Override
    public void initTopologySourceEndpoint() {
        setSourceTopic(AppProperties.getProperty("kafka.topic.analytics"));
    }

    @Override
    public void initTopologyDestinationEndpoint() {
       setTopicToRouteTo(null);
    }
}
