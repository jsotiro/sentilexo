/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sql.utils.person;

import java.util.HashMap;


/**
 *
 * @author yanni
 */
public class UserDetailsStringParser {
    
    
     static String[] termsToLookFor =   {"id", "name", "screenName"  ,"location","description", "profileImageUrl","url","isProtected","followersCount","friendsCount", "createdAt", "favouritesCount", "statusesCount", "isVerified", "listedCount"}; 
   

   
       static String[]  extractTerm(String s){
       String[] result = {"",""}; 
       String[] tempResult;
       for (String term : termsToLookFor)
           if (s.startsWith(term)){
              tempResult = s.split("=");
              
              for (int i=0;i<tempResult.length;i++)
                  if (i == 0)
                    result[0]=tempResult[0];
                  else
                   result[1]= result[1]+ tempResult[1];  
           if (result[1].startsWith("'"))
                    result[1]= result[1].replaceFirst("'", ""); 
              if (result[1].endsWith("'"))
                 result[1] = result[1].substring(0, result[1].length()-1);
              if (result[1].equals("null"))
               result[1]=null;
              
              break;
           }
       return result;
   }
  
   static HashMap parseFromSerialisedTWitter4JObject(String JsonStr){
    HashMap values = new HashMap();
    int start = JsonStr.indexOf("UserJSONImpl");
    int end = JsonStr.indexOf("isFollowRequestSent");
    JsonStr = JsonStr.substring(start, end);
    String[] JsonParts = JsonStr.split(",");
    if (JsonParts.length > 0){
        JsonParts[0] = JsonParts[0].replaceFirst("UserJSONImpl\\{","");
    }
    for (String s : JsonParts) {
        String[] temp;
        temp = extractTerm(s.trim());
        values.put(temp[0],temp[1]);
     }
    return values;
   }
}
