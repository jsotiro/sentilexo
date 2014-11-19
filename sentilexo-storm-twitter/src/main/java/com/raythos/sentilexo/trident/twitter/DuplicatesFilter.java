/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import com.raythos.sentilexo.twitter.common.domain.StatusFieldNames;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class DuplicatesFilter extends BaseFilter {
   protected  static Logger log = LoggerFactory.getLogger(DuplicatesFilter.class);
    public DuplicatesFilter() {

  }

  @Override

  public boolean isKeep(TridentTuple tuple) {
     String owner = tuple.getStringByField("owner");
     String queryName = tuple.getStringByField("queryName");
     long id = tuple.getLongByField("StatusId");
     boolean duplicate = TwitterDataManager.getInstance().statusExistsForQuery(owner, queryName, id);
     log.info("Duplicate "+duplicate +" Item "+ id + "for "+owner+","+queryName);
     if (!duplicate)
             TwitterDataManager.getInstance().insertQueryResultIndex(owner, queryName,id);
        
     return !duplicate;        
             //return idService.exists(tuple.get(0)) 
  }

    
}
