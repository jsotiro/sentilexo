package com.raythos.sentilexo.trident.twitter.state;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.map.IBackingMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SentilexoBackingMap<T> implements IBackingMap<T> {
    private static final Logger log = LoggerFactory.getLogger(SentilexoBackingMap.class);
     
    Map<String, List<T>> storage = new ConcurrentHashMap<>();
    CQLBackingStore cqlBackingStorage;

    public CQLBackingStore getCqlBackingStorage() {
        return cqlBackingStorage;
    }

    public void setCqlBackingStorage(CQLBackingStore cqlBackingStorage) {
        this.cqlBackingStorage = cqlBackingStorage;
    }
        
   
    
  /*  @Override
    @SuppressWarnings({"unchecked","rawtypes"})
    public List<T> multiGet(final List<List<Object>> keys) { {
        List<T> values = new ArrayList<>();
        for (List<Object> key : keys) {
            List<T> value = storage.get(key.get(0));
            if (value == null) {
                values.add((T) new Long(0));
            } else {
                values.add(value);
            }
        }
        return values;
    }

   /* @Override
    public void multiPut( List<List<Object>> keys, List<T> values) {    
    
        for (int i = 0; i < keys.size(); i++) {
            LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + values.get(i) + "]");
         //   storage.put((String) keys.get(i).get(0), vals.get(i));
        }
    }
*/

    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> values = new ArrayList<>();
        for (List<Object> key : keys) {
          //  List<T> value = storage.get(key.get(0));
             List<Number> value  = cqlBackingStorage.readKey(key);
            if (value == null) {
                values.add((T)(Lists.newArrayList((Number) 0L, (Number) 0L, (Number) 0L,(Number) 0L, (Number) 0L)));
            } else {
                values.add((T) value);
            }
        }
       log.info("retrieving values "+ values);
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
       for (int i = 0; i < keys.size(); i++) {
            log.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + values.get(i) + "]");
//              // storage.put((String) keys.get(i).get(0), (List<T>) values.get(i));
                   cqlBackingStorage.updateKey(keys.get(i), (List<Number>)values.get(i));
               // for each value get the config key space and cols no and update the results
        }
    }
 
}
