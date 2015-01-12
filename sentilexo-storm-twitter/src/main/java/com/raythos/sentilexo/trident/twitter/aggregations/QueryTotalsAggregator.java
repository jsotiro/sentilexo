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

package com.raythos.sentilexo.trident.twitter.aggregations;

import java.util.List;
import storm.trident.tuple.TridentTuple;
import com.google.common.collect.Lists;
import com.raythos.sentilexo.twitter.domain.QueryResultItemFieldNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.ReducerAggregator;


    
@SuppressWarnings("serial")
	 public class QueryTotalsAggregator implements ReducerAggregator<List<Number>> { 
          protected  static Logger log = LoggerFactory.getLogger(QueryTotalsAggregator.class);
               long minDate = 0L;
               long maxDate = 0L;
               long minId = 0L;
               long maxId = 0L ;        
               int totals = 0 ;
               
               long statusId;
               long dateAsLong;
                        
         
		public List<Number> init(TridentTuple tuple) {
                    // return long for date and id
                    List<Number> result;
                    
                    long statusId = tuple.getLongByField(QueryResultItemFieldNames.STATUS_ID);
                    long dateAsLong = tuple.getLongByField(QueryResultItemFieldNames.CREATED_AT);
                    
                    result = Lists.newArrayList((Number)statusId,(Number)dateAsLong );
                    log.trace("initiating combiner " +  result);
                    return result;
		}

	
		public List<Number> combine(List<Number> val1, List<Number> val2) {
                       List<Number> result;
                       statusId = (long)val2.get(0);
                       dateAsLong = (long)val2.get(1);
                       
                       minDate = (long)(val1.get(0));
                       maxDate = (long)(val1.get(1));
                       minId = (long)(val1.get(2));
                       maxId = (long)(val1.get(3));
                       totals  = (int)(val1.get(4))+1;
                       
                       minDate = (minDate==0)?dateAsLong : ( (dateAsLong<minDate)?dateAsLong:minDate );
                       maxDate = (dateAsLong>maxDate)?dateAsLong:maxDate;
                       minId =  (minId==0) ? minId : ( (statusId<minId)?statusId:minId );
                       maxId = (statusId>maxId)?statusId:maxId;
                       result= Lists.newArrayList( (Number)minDate,
                                                  (Number) maxDate, 
                                                  (Number) minId, 
                                                  (Number) maxId,
                                                  (Number)(totals) );

                       log.trace("Combining values minDate,MaxDate,MinId,MaxId, Totdals and returning" + result);
                       return result;
		}

		
		public List<Number> zero() {
                        List<Number> result = Lists.newArrayList((Number) 0L, (Number) 0L, (Number) 0L,(Number) 0L, (Number) 1L);
                        log.trace("Setting values to zero and returning " + result);
                        return result;
		}

    @Override
    public List<Number> init() {
     // read the data from the database 
         List<Number> result = Lists.newArrayList((Number) 0L, (Number) 0L, (Number) 0L,(Number) 0L, (Number) 1L);
         log.trace("initialising reducer " + result);
      return result;
        
    }

    @Override
    public List<Number> reduce(List<Number> t, TridentTuple tt) {
         List<Number> result;

         statusId = tt.getLongByField(QueryResultItemFieldNames.STATUS_ID);
         dateAsLong = tt.getLongByField(QueryResultItemFieldNames.CREATED_AT);
         
         minDate = (long)(t.get(0));
         maxDate = (long)(t.get(1));
         minId = (long)(t.get(2));
         maxId = (long)(t.get(3));
         totals  = (int) (t.get(4))+1;
                       
         minDate = (minDate==0)?dateAsLong : ( (dateAsLong<minDate)?dateAsLong:minDate );
         maxDate = (dateAsLong>minDate)?dateAsLong:maxDate;
         minId =  (minId==0) ? statusId : ( (statusId<minId)?statusId:minId ); 
         maxId = (statusId>maxId)?statusId:maxId;
         result= Lists.newArrayList( (Number)minDate,
                                     (Number)maxDate, 
                                     (Number) minId, 
                                     (Number) maxId,
                                     (Number)(totals) );

        log.info("Updating values minDate,MaxDate,MinId,MaxId, Totdals and returning" + result);
        return result;   
        
        
    }
}