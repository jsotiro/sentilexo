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
package com.raythos.sentilexo.persistence.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 *
 * @author yanni
 */
public class DataConnected {
    private Session session; 
    private ResultSet lastResultSet;

    public Session getSession() {
        if (session==null)
            this.session = DataManager.getInstance().getSession();     
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }
    
  public  DataConnected(){
      super();
     
    }

    public ResultSet getLastResultSet() {
        return lastResultSet;
    }

    protected void setLastResultSet(ResultSet lastResultSet) {
        this.lastResultSet = lastResultSet;
    }
    
    
  
    
}
