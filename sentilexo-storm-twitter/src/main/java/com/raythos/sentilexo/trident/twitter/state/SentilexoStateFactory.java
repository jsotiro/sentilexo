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

package com.raythos.sentilexo.trident.twitter.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class SentilexoStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;
        CqlStorageConfigValues config;
    public SentilexoStateFactory(CqlStorageConfigValues config){
        this.config = config;
        
    }
    
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        SentilexoBackingMap map = new SentilexoBackingMap();
        map.setCqlBackingStorage(new CQLBackingStore(config.getTable(),config.getKeySpec(),config.getFieldSpec()));
        return new SentilexoState(map);
    }
}
