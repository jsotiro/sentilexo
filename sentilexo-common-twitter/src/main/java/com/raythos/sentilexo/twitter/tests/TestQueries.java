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

package com.raythos.sentilexo.twitter.tests;

import com.datastax.driver.core.Session;
import com.raythos.sentilexo.persistence.cql.DataManager;
import com.raythos.sentilexo.twitter.domain.Queries;
import com.raythos.sentilexo.twitter.domain.Query;
import com.raythos.sentilexo.common.utils.AppProperties;
import java.util.List;

/**
 *
 * @author yanni
 */
public class TestQueries {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        DataManager.getInstance().connect(
                AppProperties.getProperty("cqlhost"),AppProperties.getProperty("cqlschema") );
        
                Session session = DataManager.getInstance().getSession();
                
                Query q = new Query();
                q.setOwner("john");
                q.setQueryName("ukip");
                q.save();
                q.setOwner("john");
                q.setQueryName("labour");   
                q.save();
                q.setOwner("john");
                q.setQueryName("all parties");
                q.save();
                Queries tqs = new Queries(session);
                List<Query> queries = tqs.getQueries(null);
                for (Query qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
                q.setOwner("ray");
                q.setQueryName("My brand");
                q.save();
                 queries = tqs.getQueries(null);
                for (Query qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
                 queries = tqs.getQueries("john");
                for (Query qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
 

    }
    
}
