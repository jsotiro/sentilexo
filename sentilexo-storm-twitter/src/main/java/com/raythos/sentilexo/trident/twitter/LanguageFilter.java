/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
public class LanguageFilter extends BaseFilter {
    private final String[] languages;
 
    public LanguageFilter(String includedLanguages[]) {

    this.languages = includedLanguages;

  }

  @Override

  public boolean isKeep(TridentTuple tuple) {
    boolean result = false;
    
    String value = tuple.getStringByField("lang").toLowerCase();
    for (String lang : languages ){
        if (lang.equals(lang.toLowerCase())) {
          result = true;
          break;
        }
    }
    return result;
  }

    
}
