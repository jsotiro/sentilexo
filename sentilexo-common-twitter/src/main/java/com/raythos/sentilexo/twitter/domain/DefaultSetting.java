/*
 * Copyright 2014 Expression project.organization is undefined on line 4, column 57 in Templates/Licenses/license-apache20.txt..
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
package com.raythos.sentilexo.twitter.domain;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.raythos.sentilexo.persistence.cql.PersistedEntity;
import java.util.List;

/**
 *
 * @author yanni
 */
public class DefaultSetting extends PersistedEntity {
    private String name;
    List<String> values;

    public DefaultSetting(){
        super();
    }
    
    public DefaultSetting(String name, List values){
        super();
        this.name = name;
        this.values = values;
    }
    
    public String getName() {
        return name;
    }

   
    public void setName(String name) {
        this.name = name;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
    
    @Override
    public void valuesFromRow(Row row) {
        values = row.getList(0, String.class);
    }

    @Override
    public void bindCQLLoadParameters(BoundStatement boundStm) {
         boundStm.bind(name);
    }

    @Override
    public void bindCQLSaveParameters(BoundStatement boundStm) {
         boundStm.bind(name,values);
    }
    
}
