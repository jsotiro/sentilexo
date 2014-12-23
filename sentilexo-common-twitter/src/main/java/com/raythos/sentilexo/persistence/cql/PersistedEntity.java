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
import java.io.Serializable;

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

    @Override
    public abstract void valuesFromRow(Row row); 
 
    @Override
    public abstract void bindCQLLoadParameters(BoundStatement boundStm);

    @Override
    public abstract void bindCQLSaveParameters(BoundStatement boundStm);
}
