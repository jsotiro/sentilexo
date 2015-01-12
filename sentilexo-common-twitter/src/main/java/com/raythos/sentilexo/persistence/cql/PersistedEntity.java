/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com
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
package com.raythos.sentilexo.persistence.cql;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileParser;
import com.raythos.sentilexo.twitter.domain.QueryResultItemMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public abstract class  PersistedEntity implements Serializable, Persistable {
         
    public boolean load(){
      return DataManager.getInstance().getPersisterFor(this).load(this);
   }
   
   public void save() {
       DataManager.getInstance().getPersisterFor(this).save(this);
   }  

   public static byte[] toBinaryJSon(PersistedEntity item){
          try {
              SmileFactory f = new SmileFactory();
              f.configure(SmileParser.Feature.REQUIRE_HEADER, true);
              ObjectMapper mapper = new ObjectMapper(f);
                          mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
              byte[] result = mapper.writeValueAsBytes(item);
              return result;
          } catch (JsonProcessingException ex) {
              Logger.getLogger(QueryResultItemMapper.class.getName()).log(Level.SEVERE, null, ex);
              return null;
              
          }
      }
   
     public static PersistedEntity fromBinaryJSon(byte[] data, Class classType ){
        try {
            SmileFactory f = new SmileFactory();
            f.configure(SmileParser.Feature.REQUIRE_HEADER, true);
            
            ObjectMapper mapper = new ObjectMapper(f);
            
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            Object result = mapper.readValue(data, classType);
            return (PersistedEntity)result;
        } catch (IOException ex) {
            Logger.getLogger(PersistedEntity.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
       }
   
    @Override
    public abstract void valuesFromRow(Row row); 
 
    @Override
    public abstract void bindCQLLoadParameters(BoundStatement boundStm);

    @Override
    public abstract void bindCQLSaveParameters(BoundStatement boundStm);
}
