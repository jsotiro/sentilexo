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

package com.raythos.sentilexo.state;

import com.raythos.sentilexo.persistence.cql.KeyValues;
import com.raythos.sentilexo.persistence.cql.DataManager;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class CQLBackingStore {
   
  
   
   
   String keyName;
   Map<String, Integer> keySpec;
   Map<String, Integer> fieldSpec;
   
   DataManager dataManager;
   KeyValues cqlStorage; 
   
   public CQLBackingStore(String table, Map<String, Integer> keySpec, Map<String, Integer> fieldSpec){
      dataManager = DataManager.getInstance();
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
