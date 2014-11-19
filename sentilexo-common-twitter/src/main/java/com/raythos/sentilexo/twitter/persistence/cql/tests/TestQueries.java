/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.twitter.persistence.cql.tests;

import com.datastax.driver.core.Session;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterQueries;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterQuery;
import com.raythos.sentilexo.utils.AppProperties;
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
        TwitterDataManager.getInstance().connect(
                AppProperties.getProperty("cqlhost"),AppProperties.getProperty("cqlschema") );
                Session session = TwitterDataManager.getInstance().getSession();
                TwitterQuery q = new TwitterQuery(session);
                q.setOwner("john");
                q.setQueryName("ukip");
                q.save();
                q.setOwner("john");
                q.setQueryName("labour");   
                q.save();
                q.setOwner("john");
                q.setQueryName("all parties");
                q.save();
                TwitterQueries tqs = new TwitterQueries(session);
                List<TwitterQuery> queries = tqs.getQueries(null);
                for (TwitterQuery qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
                q.setOwner("ray");
                q.setQueryName("My brand");
                q.save();
                 queries = tqs.getQueries(null);
                for (TwitterQuery qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
                 queries = tqs.getQueries("john");
                for (TwitterQuery qObj : queries) { System.out.println(qObj.getOwner() + " "+qObj.getQueryName()); }
 

    }
    
}
