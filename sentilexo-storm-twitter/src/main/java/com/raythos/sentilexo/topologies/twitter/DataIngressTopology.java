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

import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.common.utils.AppProperties;
import com.raythos.sentilexo.state.SentilexoStateFactory;
import com.raythos.sentilexo.topologies.base.CoreTopology;
import com.raythos.sentilexo.trident.UpdateTopologiesJournal;
import com.raythos.sentilexo.trident.twitter.AvroBytesToResultItemFunction;
import com.raythos.sentilexo.trident.twitter.DuplicatesFilter;
import com.raythos.sentilexo.trident.twitter.ExtractStatsFields;
import com.raythos.sentilexo.trident.twitter.LanguageFilter;
import com.raythos.sentilexo.trident.twitter.TextLineToResultItemFunction;
import com.raythos.sentilexo.trident.twitter.aggregations.QueryTotalsAggregator;
import com.raythos.sentilexo.trident.twitter.state.QueryStatsCqlStorageConfigValues;
import com.raythos.sentilexo.twitter.domain.DefaultSetting;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import storm.trident.Stream;
import storm.trident.TridentState;

/**
 *
 * @author John Sotiropoulos
 */


public class DataIngressTopology extends CoreTopology {

     private static final String[] languagesToAccept = {"en","und"};
     private static final Fields groupByFields = new Fields(QueryResultItemFieldNames.QUERY_OWNER, QueryResultItemFieldNames.QUERY_NAME);
     private static final Fields totalItemsFields = new Fields("totalitems");

   
    
    @Override
    public void updateTrackingInfo(){
        DefaultSetting mainTopologyName = new DefaultSetting("main_topology", null );         
        mainTopologyName.addValue(getTopologyName());
        mainTopologyName.save();
        super.updateTrackingInfo();
    } 
     
    @Override
    public void defineFunctions() {
               
 
        Stream queryStream = getMainStream().each(CoreTopology.itemField, new LanguageFilter(languagesToAccept))
                               .each(CoreTopology.itemField, new DuplicatesFilter(getTopologyName()));
                 
        TridentState queryStatsState =  
                                   queryStream.each(itemField, new  ExtractStatsFields(),ExtractStatsFields.statsFields )
                                   .groupBy(groupByFields)
                                   .persistentAggregate(new SentilexoStateFactory(new QueryStatsCqlStorageConfigValues()),
                                    ExtractStatsFields.statsFields, 
                                    new QueryTotalsAggregator(), 
                                    totalItemsFields);  
    }    

    @Override
    public void initDeserializerFunctions() {
       this.setBinaryDeserializerFunction(new AvroBytesToResultItemFunction());
       this.setTextDeserializerFunction(new TextLineToResultItemFunction());
    
    }


    @Override
    public void initTopologySourceEndpoint() {
        setSourceTopic(AppProperties.getProperty("kafka.topic.results"));
    }

    @Override
    public void initTopologyDestinationEndpoint() {
       setTopicToRouteTo(AppProperties.getProperty("kafka.topic.analytics"));
    }
    
     public static void main(String[] args)  {
        
         DataIngressTopology data_topology = new  DataIngressTopology();
         data_topology.configFromArgs(args);
         data_topology.execute();        
    } 
    
 }

