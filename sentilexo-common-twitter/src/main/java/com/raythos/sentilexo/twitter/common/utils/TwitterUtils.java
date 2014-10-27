/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.twitter.common.utils;

import java.util.ArrayList;
import java.util.List;
import twitter4j.Status;

/**
 *
 * @author yanni
 */
public class TwitterUtils {
    
   public static  String queryTermsInStatus(String searchQueries, String statusMsg){        
        StringBuilder result = new StringBuilder("");
        String[] queryTerms = searchQueries.split("\\s*,\\s*");
        for (String s : queryTerms)
            if ( statusMsg.toLowerCase().contains(s.toLowerCase()) ){
                result.append(s);
                result.append(",");
            }
        if (result.length() >0 ) 
            result.deleteCharAt(result.length()-1);
        return result.toString();
    }
      
   public static  String relevantQueryTextFromStatus(String searchQueries, Status status){
       String query;
       String statusText;
       if (status.getRetweetedStatus()!=null)
           statusText = status.getRetweetedStatus().getText();
       else
           statusText = status.getText();
        query =  queryTermsInStatus(searchQueries, statusText);   
       return query;
    }
   
     public static  List<String> queryTermsListInStatus(String searchQuery, String statusMsg){        
        ArrayList<String> result = new ArrayList<>();
        String[] queryTerms = searchQuery.split("\\s*,\\s*");
        for (String s : queryTerms)
            if ( statusMsg.toLowerCase().contains(s.toLowerCase()) ){
                result.add(s);
            }
        return result;
    }
   
   
   public static  List<String> relevantQueryTermsFromStatus(String searchQuery, Status status){
       List<String> result;
       String statusText;
       if (status.getRetweetedStatus()!=null)
           statusText = status.getRetweetedStatus().getText();
       else
           statusText = status.getText();
        result =  queryTermsListInStatus(searchQuery, statusText);   
       return result;
    }
    
}
