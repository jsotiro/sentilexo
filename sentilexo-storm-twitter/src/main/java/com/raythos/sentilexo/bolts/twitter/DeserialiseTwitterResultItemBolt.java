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

package com.raythos.sentilexo.bolts.twitter;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;


public class DeserialiseTwitterResultItemBolt extends BaseBasicBolt {


    public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("StatusId","ResultItem"));
    }
    
    
    transient Schema avroSchema = null;
        protected TwitterQueryResultItemAvro deserializeBinary(byte[] bytes) {
            if (avroSchema == null) {
                    Schema.Parser parser = new Schema.Parser();
                    try {
                        InputStream in = getClass().getResourceAsStream("/TwitterResultSchema.avsc");
                        avroSchema = parser.parse(in);
                         in.close();
                        in = null;
                    } catch (IOException e1) {
                        
                        Logger.getLogger(TwitterQueryResultItemAvro.class.getName()).log(Level.SEVERE , null,e1);
                        
			}
		}

		TwitterQueryResultItemAvro result = null;
		try {
                       DatumReader<TwitterQueryResultItemAvro> reader = new SpecificDatumReader<>(avroSchema,avroSchema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                        //Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema,json);
			result = reader.read(null, decoder);
                        reader = null;
                        avroSchema = null;
                        
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		}
		return result;
	}

    
  
	protected TwitterQueryResultItemAvro deserialize(String json) {
            if (avroSchema == null) {
                    Schema.Parser parser = new Schema.Parser();
                    try {
                        InputStream in = getClass().getResourceAsStream("/TwitterResultSchema.avsc");
                        avroSchema = parser.parse(in);
                        in.close();
                        in = null;
                    } catch (IOException e1) {
                        
                        Logger.getLogger(TwitterQueryResultItemAvro.class.getName()).log(Level.SEVERE , null,e1);
                        
			}
		}

		TwitterQueryResultItemAvro result = null;
		try {
                       DatumReader<TwitterQueryResultItemAvro> reader = new SpecificDatumReader<>(avroSchema,avroSchema);
			//Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                        Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema,json);
			result = reader.read(null, decoder);
                        reader = null;
                        
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		}
		return result;
	}

    

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      byte[] bytes= tuple.getBinary(0);
   
        
        TwitterQueryResultItemAvro tqri = deserializeBinary(bytes);
        collector.emit(new Values(tqri.getStatusId(),tqri ));
        
    }
    
}


