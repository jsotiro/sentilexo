/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter.state;

import com.raythos.sentilexo.twitter.persistence.cql.KeyValues;

/**
 *
 * @author yanni
 */
public class QueryStatsCqlStorageConfigValues extends CqlStorageConfigValues{
    
    
    public QueryStatsCqlStorageConfigValues(){
     super();
     keySpec.put("owner", KeyValues.STRING);
     keySpec.put("queryname", KeyValues.STRING);
             
     fieldSpec.put("earliest", KeyValues.DATE);
     fieldSpec.put("latest", KeyValues.DATE);
     fieldSpec.put("minid", KeyValues.LONG);
     fieldSpec.put("maxid", KeyValues.LONG);
     fieldSpec.put("totalitems", KeyValues.INT);
     this.setTable("queries");
    }

 
}
