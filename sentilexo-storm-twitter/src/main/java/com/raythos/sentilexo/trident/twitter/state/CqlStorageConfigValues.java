/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter.state;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class CqlStorageConfigValues implements Serializable {
   
    protected Map<String,Integer> keySpec = new LinkedHashMap();
    protected Map<String,Integer> fieldSpec = new LinkedHashMap();
    private String table;

    public Map<String, Integer> getKeySpec() {
        return keySpec;
    }

    public void setKeySpec(Map<String, Integer> keySpec) {
        this.keySpec = keySpec;
    }

    public Map<String, Integer> getFieldSpec() {
        return fieldSpec;
    }

    
    public void setFieldSpec(Map<String, Integer> fieldSpec) {
        this.fieldSpec = fieldSpec;
    }
        
    public String getTable() {
        return table;
    }
    public void setTable(String table) {
        this.table = table;
    } 
        
}
