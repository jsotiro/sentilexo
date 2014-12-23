/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com
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

package com.raythos.sentilexo.common.utils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author yanni
 */
public class Helpers {
  
    public static final String  escapedDoubleQutes = "\"";
    
    public static String quotedString(String text){
         String s=  escapedDoubleQutes + text + escapedDoubleQutes;
         return s;
    }    
    
    public static String toStringIfNotNull(Object o){
     if (o!=null)
         return o.toString();
     else
         return null;
    }
    
     public static List cloneList(List l){
        ArrayList<Object> list = new ArrayList();
        if (l==null) return null; 
        for (Object o : l){
            list.add(o);
        }
        return list;
    }
  
}
