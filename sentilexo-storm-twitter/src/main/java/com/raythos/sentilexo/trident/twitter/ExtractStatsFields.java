/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.twitter.common.domain.StatusFieldNames;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class ExtractStatsFields  extends BaseFunction{
    protected  static Logger log = LoggerFactory.getLogger(ExtractStatsFields.class);
   
     static final Fields statsFields = new Fields("statusId","createdAt");
  
    
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        
        Long statusId = (long)tuple.getLongByField("StatusId");
        try {
            Map<String,Object> result = (Map) tuple.getValueByField("ResultItem");
        
            Date resultCreationDate = new Date(); 
            resultCreationDate.setTime(((Date)result.get(StatusFieldNames.CREATED_AT)).getTime());
            List<Object> v = new ArrayList<>(); 
        //     v.add("raythos"); // will param in the next iteration - for now only one owner 
            // v.add(query); 
             v.add(statusId); 
             v.add(resultCreationDate.getTime()); 
                    
                    
          collector.emit(v);
          log.trace("StatusId "+statusId+" emiting stats fields to aggregator");         
          //result = null; 
          }
        catch (Exception e)     {
            log.error("error when emiting stats fields to aggregator for statusId "+statusId +". Error msg "+e);
        }
    }   

}
