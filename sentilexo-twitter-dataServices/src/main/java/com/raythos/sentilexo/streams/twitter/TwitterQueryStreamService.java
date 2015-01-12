/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.streams.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author yanni
 */
public class TwitterQueryStreamService {
 static final Logger log = LoggerFactory.getLogger(TwitterQueryStreamService.class);
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
       log.info("Starting Twitter Query Stream Service");
        
        TwitterQueryStream stream = new TwitterQueryStream();
        try {
            
            stream.setup();
            stream.connect();
            stream.run();
        } catch (Exception ex) {
            log.error("Error while listening to twitter stream for query "+stream.getSearchQueries(), ex);
        }
    }
    
}
