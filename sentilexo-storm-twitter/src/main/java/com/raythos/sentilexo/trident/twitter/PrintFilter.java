/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 *
 * @author yanni
 */
@SuppressWarnings({ "serial", "rawtypes" })
	public class PrintFilter implements Filter {
        protected  static Logger log = LoggerFactory.getLogger(PrintFilter.class);
                int items = 0;
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
                }
		@Override
		public void cleanup() {
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			//log.info("processing tuple "+tuple.getValues());
                         items ++;
                        System.out.println(tuple + "  /" + items );
			return true;
		}


    
}
