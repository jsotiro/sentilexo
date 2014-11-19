/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter.aggregations;

import java.util.List;
import storm.trident.tuple.TridentTuple;
import com.google.common.collect.Lists;
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
                    
                    long statusId = tuple.getLong(1);
                    long dateAsLong = tuple.getLong(2);
                    result = Lists.newArrayList((Number)statusId,(Number)dateAsLong );
                    log.info("initiating combiner " +  result);
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

                       log.info("Combining values minDate,MaxDate,MinId,MaxId, Totdals and returning" + result);
                       return result;
		}

		
		public List<Number> zero() {
                        List<Number> result = Lists.newArrayList((Number) 0L, (Number) 0L, (Number) 0L,(Number) 0L, (Number) 1L);
                        log.info("Setting values to zero and returning " + result);
                        return result;
		}

    @Override
    public List<Number> init() {
     // read the data from the database 
         List<Number> result = Lists.newArrayList((Number) 0L, (Number) 0L, (Number) 0L,(Number) 0L, (Number) 1L);
         log.info("initialising reducer " + result);
      return result;
        
    }

    @Override
    public List<Number> reduce(List<Number> t, TridentTuple tt) {
         List<Number> result;
         statusId = (long)tt.get(0);
         dateAsLong = (long)tt.get(1);
         
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