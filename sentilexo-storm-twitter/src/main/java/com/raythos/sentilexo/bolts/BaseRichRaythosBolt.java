/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yanni
 */
public class BaseRichRaythosBolt extends BaseRichBolt{
    protected Logger log = LoggerFactory.getLogger(BaseCQLBolt.class);
    protected OutputCollector collector;
     
    public Logger getLog() {
        return log;
    }

    public OutputCollector getCollector() {
        return collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        throw new UnsupportedOperationException("Not supported - you should not call a BaseRichRaythosBolt directly."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
      collector = oc;
    }

    @Override
    public void execute(Tuple input) {
        throw new UnsupportedOperationException("Not supported - you should not call a BaseRichRaythosBolt directly."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
