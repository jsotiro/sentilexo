/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.bolts;

import com.raythos.sentilexo.bolts.BaseRichRaythosBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.util.Map;

/**
 *
 * @author yanni
 */
public class BaseCQLBolt   extends BaseRichRaythosBolt{
    protected String cqlHost="localhost";
    protected String cqlKeyspace="twitterqueries";
  
  
     @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
 @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
      super.prepare(stormConf, context, oc);
     // cqlHost = AppProperties.getProperty("cqlhost", "localhost");
      //cqlKeyspace = AppProperties.getProperty( cqlKeyspace, "twitterqueries");
      TwitterDataManager.getInstance().connect(cqlHost, cqlKeyspace);
    }

     @Override
    public void execute(Tuple input) {
     }
    
}
