/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com *
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

import com.raythos.sentilexo.persistence.cql.DataConnected;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.raythos.sentilexo.common.utils.DateTimeUtils;
import com.raythos.sentilexo.persistence.cql.RegisteredPreparedStatements;
import java.util.Date;

/**
 *
 * @author yanni
 */
public class SentimentTotals extends DataConnected {
  
    public void  updateSentimentTotalsCQL(String method, String owner, String queryName, String sentiment, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement  boundUpdateSentimentTotals; 
        PreparedStatement stmt;
        switch (sentiment) {
             case "YES":
                   if (retweet==0) stmt = RegisteredPreparedStatements.updateSentimentTotalsYesRetweets;
                   else stmt = RegisteredPreparedStatements.updateSentimentTotalsYesNoRetweets ;        
                 break;
             case "NO":
                     if (retweet==0) stmt =RegisteredPreparedStatements.updateSentimentTotalsNoRetweets;
                     else stmt =RegisteredPreparedStatements.updateSentimentTotalsNoNoRetweets;        
                 break;
             default:
                 if (retweet==0) stmt =RegisteredPreparedStatements.updateSentimentTotalsUnlcearRetweets;
                 else stmt =RegisteredPreparedStatements.updateSentimentTotalsUnlcearNoRetweets;   
             break;
         } 
        boundUpdateSentimentTotals = new BoundStatement(stmt);
        boundUpdateSentimentTotals.bind(1L,1L,1L,1L,method, owner, queryName,period_type,time_id);
        ResultSet results = getSession().execute(boundUpdateSentimentTotals);
        setLastResultSet(results);
        
        
}
   
   public void updateSentimentTotals(String method, String owner, String query, String sentiment, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateSentimentTotalsCQL(method,owner, query,sentiment, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
   
   
    }
   
}
