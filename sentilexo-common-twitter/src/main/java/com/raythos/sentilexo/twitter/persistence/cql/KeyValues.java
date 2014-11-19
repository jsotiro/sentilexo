/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.twitter.persistence.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import com.datastax.driver.core.querybuilder.Select;
import com.raythos.sentilexo.utils.DateTimeUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class KeyValues {
     public static final int INT  =   0;
    public static final int LONG =    1;
    public static final int FLOAT  =  2;
    public static final int DOUBLE  = 3;
    public static final int BOOLEAN = 4;
    public static final int DATE  =   5;
    public static final int STRING  = 6;
    public static final int BYTES  =  7;
 
    
    
    static Logger log = LoggerFactory.getLogger(KeyValues.class); 
    private Session session;
    String keyspace; 
    String table;
    Map<String, Integer> fieldSpec;
            
    public KeyValues(Session session){
        super();
        setSession(session);
    }

    public KeyValues(Map<String, Integer> fieldSpecs,  String table,  String keyspace, Session session){
        super();
        setSession(session);
        setFieldSpec(fieldSpecs);
        setKeyspace(keyspace);
        setTable(table);
    }

    
    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Integer> getFieldSpec() {
        return fieldSpec;
    }

    public void setFieldSpec(Map<String, Integer> fieldSpec) {
        this.fieldSpec = fieldSpec;
    }
    
    public void writeKeyValues(Map<String,Object> keys, Map<String,Object> values) {
      
        Insert insert = QueryBuilder.insertInto(keyspace, table);
        for (String k : keys.keySet()){
             insert.value(k, keys.get(k));
        }
        for (String k : values.keySet()){
             insert.value(k, values.get(k));
        }
      session.execute(insert);
        
    }
        
    public List<Object> readKeyValues(Map<String,Object> keys)   {
        List<Object>  result  = new ArrayList<>();
        Select select = QueryBuilder.select().all().from(keyspace,table);
        int i = 0;
        for (String k : keys.keySet()){
           if (i==0)
               select.where(eq(k, keys.get(k)));
            else
               select.where().and(eq(k, keys.get(k)));
           i++;
        }
        String qs = select.getQueryString();
        log.trace(qs);
        ResultSet results = session.execute(select);
        List<Row> rows  = results.all();
        int count = rows.size();
        if (count==0)
        {
           return null;     
        }
        else
        {
            Row firstRow = rows.get(0);
            for (String k : fieldSpec.keySet()){
              int type = fieldSpec.get(k);
               if (type == INT) // int
                 result.add(firstRow.getInt(k));
               else if (type == LONG) // long 
                result.add(firstRow.getLong(k));
        
                else if (type == FLOAT) // float
                 result.add(firstRow.getFloat(k));
                else if (type == DOUBLE) // double
                 result.add(firstRow.getDouble(k));
                else if (type == BOOLEAN) // bool
                 result.add(firstRow.getBool(k));
                else if (type == DATE) // date
                 result.add(firstRow.getDate(k));
                else if (type == STRING) // string
                 result.add(firstRow.getString(k));
                else 
                    result.add(firstRow.getBytes(k));
            } 
        }    
        return result;
    } 
    
}
