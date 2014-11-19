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
