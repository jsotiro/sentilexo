/*
 * Copyright 2014 (c) Raythos Interactive Ltd.  http://www.raythos.com
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
