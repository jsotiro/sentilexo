/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter.state;

import com.raythos.sentilexo.twitter.persistence.cql.KeyValues;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author yanni
 */
public class CQLBackingStore {
   
  
   
   
   String keyName;
   Map<String, Integer> keySpec;
   Map<String, Integer> fieldSpec;
   
   TwitterDataManager dataManager;
   KeyValues cqlStorage; 
   
   public CQLBackingStore(String table, Map<String, Integer> keySpec, Map<String, Integer> fieldSpec){
      dataManager = TwitterDataManager.getInstance();
      this.keySpec = keySpec;
      this.fieldSpec = fieldSpec;
      cqlStorage = new KeyValues(fieldSpec,table,dataManager.getKeyspace(),dataManager.getSession() );
   }
    
   public List<Number> readKey(List<Object> keyValues){
       List<Number> result= new ArrayList<>();
       Map<String, Object> fullKeyValues = new HashMap();
       int i = 0;
       for (String k : keySpec.keySet()){
           fullKeyValues.put(k, keyValues.get(i));
           i++;
       }
       List<Object> cqlResults = cqlStorage.readKeyValues(fullKeyValues);
       if  (cqlResults==null)
           result = listOfZeros();
       else
        for (Object obj : cqlResults) {
           if (obj instanceof Date)
              result.add(((Date)obj).getTime());
           else
               if (obj instanceof  Number)
                    result.add((Number)obj);
               else if (obj==null ) {
                   result.add(0L);
               }
              //ignore anything non numeric / date
         
       }
       
       // data manager read
        // readKeys(Keys,Values)
       // convert dates to number
       
       return result; 
   }
   
   public void updateKey(List<Object> keyValues, List<Number> items){
      // convert dates to long 
       Map<String, Object> fullKeyValues = new HashMap();
       Map<String, Object> fullValues = new HashMap();
       int i = 0;
      
       
       for (String k : keySpec.keySet() ){
         fullKeyValues.put(k, keyValues.get(i));
         i++;
       }
       i = 0;
       for (String k : fieldSpec.keySet() ){
          if (fieldSpec.get(k)==KeyValues.DATE) { 
             Date d = new Date();
             d.setTime(items.get(i).longValue());
             fullValues.put(k,d ); 
          }
          else
             fullValues.put(k,items.get(i).longValue());
         i++;
       }
       cqlStorage.writeKeyValues(fullKeyValues, fullValues);
       
   }

    private List<Number> listOfZeros() {
        ArrayList<Number> result = new ArrayList<>();
        for (String k : fieldSpec.keySet())            
          if (fieldSpec.get(k)==KeyValues.LONG || fieldSpec.get(k)==KeyValues.DATE )    
            result.add(0L);
          else
            result.add(0); 
      return result;    
    }
}
