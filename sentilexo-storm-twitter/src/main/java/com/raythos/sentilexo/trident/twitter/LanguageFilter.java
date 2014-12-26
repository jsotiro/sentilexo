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

    // no filtering on language is needed
    if (languages==null || languages.length <1 ){
      return true;   
    } 

    // filtering on language is needed
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
