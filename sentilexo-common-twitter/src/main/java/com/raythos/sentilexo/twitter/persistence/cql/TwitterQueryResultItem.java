package com.raythos.sentilexo.twitter.persistence.cql;
/**
 *
 * @author yanni
 */


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.common.domain.TwitterQueryResultItemMapper;
import com.raythos.sentilexo.utils.DateTimeUtils;
import com.raythos.sentilexo.utils.Helpers;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwitterQueryResultItem {
        static Logger log = LoggerFactory.getLogger(DateTimeUtils.class); 
   
    
    public static Insert buildInsertJsonCQL(long StatusId, String json, String keyspace){
       Insert insert;
        insert = (Insert) QueryBuilder.insertInto(Helpers.quotedString(keyspace),Helpers.quotedString("jsonlog"))
            .value(Helpers.quotedString("StatusId"), StatusId)
            .value(Helpers.quotedString("json"), json)
          .setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
        
        return insert;
    }
    
    
    
    public static Insert buildInsertCQL(Map<String,Object> fields, String keyspace){
       Insert insert;
       insert = (Insert) QueryBuilder.insertInto(Helpers.quotedString(keyspace), Helpers.quotedString("twitter_data"));
       for(String key: fields.keySet()){
            insert.value(Helpers.quotedString(key), fields.get(key));
       }
       insert.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
     return insert;
}
 
    
 
    
} 
 