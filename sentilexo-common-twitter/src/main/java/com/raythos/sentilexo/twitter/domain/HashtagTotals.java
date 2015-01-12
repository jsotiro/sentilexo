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
import com.datastax.driver.core.ResultSet;
import com.raythos.sentilexo.common.utils.DateTimeUtils;
import com.raythos.sentilexo.persistence.cql.RegisteredPreparedStatements;
import java.util.Date;

/**
 *
 * @author yanni
 */
public class HashtagTotals extends DataConnected {
    
    public void  updateHashtatgTotalsCQL(String owner,String queryName, String hashtag, int period_type, String period,  Date time_id, int retweet){   
        BoundStatement boundUpdateTotalStatement = new BoundStatement(RegisteredPreparedStatements.updateHashTagTotals);
        BoundStatement boundUpdateTotalByRetweetStatement;
        
        if (retweet == 0)
            boundUpdateTotalByRetweetStatement = new BoundStatement(RegisteredPreparedStatements.updateHashTagNonRetweetTotals);
        else
            boundUpdateTotalByRetweetStatement =  new BoundStatement(RegisteredPreparedStatements.updateHashTagRetweetTotals);
        
        
        boundUpdateTotalStatement.bind(1L,owner, queryName,period_type,time_id,hashtag);
        boundUpdateTotalByRetweetStatement.bind(1L,owner, queryName,period_type,time_id,hashtag);
            
        ResultSet execute = this.getSession().execute(boundUpdateTotalStatement);
        execute = getSession().execute(boundUpdateTotalByRetweetStatement);
        
}

   
    
    public void updateHashTagTotals(String owner, String query, String hashtag, Date createdAt, int retweet) {
    for (int i=-1;i<DateTimeUtils.YEAR+1;i++){
            this.updateHashtatgTotalsCQL(owner, query,hashtag, i,
                                    DateTimeUtils.getBucket(i, createdAt),
                                    DateTimeUtils.getBucketDateTime(i, createdAt) , 
                                    retweet);
  
    }    
    }   
    
}
