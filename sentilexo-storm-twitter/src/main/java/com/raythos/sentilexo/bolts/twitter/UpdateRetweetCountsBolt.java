/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.bolts.twitter;

import backtype.storm.tuple.Tuple;
import com.raythos.sentilexo.bolts.BaseCQLBolt;

/**
 *
 * @author yanni
 */
public class UpdateRetweetCountsBolt extends BaseCQLBolt {
    
    
    @Override
    public void execute(Tuple input) {
       /*
          
         if retweetId exists as statusId update the record with setting retweeted to yes and incrememt the counter
         if not insert the original and update 
        
        */
        
     }
    
}
