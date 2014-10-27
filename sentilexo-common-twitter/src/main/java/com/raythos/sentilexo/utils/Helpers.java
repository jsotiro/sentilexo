/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.utils;

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
  
}
