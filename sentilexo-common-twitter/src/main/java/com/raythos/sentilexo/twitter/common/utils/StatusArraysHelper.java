/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.twitter.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;

/**
 *
 * @author yanni
 */
public class StatusArraysHelper {
    
    
 
 public  static  List<String> getHashTagsList(Status status){
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        List<String>  result = new ArrayList<>();
        for (HashtagEntity h : status.getHashtagEntities()) {
            result.add(h.getText().toLowerCase());
        }
        return result;       
    }
    
   public static  Map<String, Long> getUserMentionMap(Status status){
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        Map<String, Long> result = new HashMap<>();
        for (UserMentionEntity um : status.getUserMentionEntities()) {
            result.put(um.getScreenName(),um.getId());
        }
        return result;       
    }
    
      
    public static  List<String> getUrlsList(Status status){
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    
         List<String> result = new ArrayList<>();
        for (URLEntity url : status.getURLEntities()) {
            result.add(url.getURL());
        }
        return result;       
    }      
   
     
       
}
