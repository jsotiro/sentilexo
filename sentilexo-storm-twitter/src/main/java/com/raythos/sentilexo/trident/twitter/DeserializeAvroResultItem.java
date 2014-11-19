/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.trident.twitter;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.common.domain.TwitterQueryResultItemMapper;
import com.raythos.sentilexo.twitter.persistence.cql.TwitterDataManager;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


/**
 *
 * @author yanni
 */
public class DeserializeAvroResultItem  extends BaseFunction {
    
     protected static org.slf4j.Logger   log = LoggerFactory.getLogger(DeserializeAvroResultItem.class);
     
     public static final Fields avroObjectFields = new Fields("owner","queryName","StatusId","ResultItem","lang");
     
     void calculateAndUpdateQueryStats(){
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
                        
                        log.error("Error reading Avro Scheme TwitterResultSchema.avsc. Error msg: " + e1);
                        
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
		        log.error("Error Deserialising TwitterResultItemAvro instance  Error msg: " + e);
			throw new RuntimeException(e);
		}
		return result;
	}
    
         
        
   @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
             
             byte[] bytes= tuple.getBinary(0);
             TwitterQueryResultItemAvro result = deserializeBinary(bytes);
             Map fields = TwitterQueryResultItemMapper.asFieldMap(result);
             TwitterDataManager.getInstance().saveTwitterQueryResultItem(fields);
             Long statusId = result.getStatusId();
             String queryName = result.getQueryName();
             if (queryName==null)
                 queryName = "indyref";
             String clKeyspace = TwitterDataManager.getInstance().getKeyspace();
             log.trace("Result Item StatusId = "+ statusId + " written to Cassandra keyspace"+clKeyspace);
             collector.emit(new Values("raythos", queryName, statusId,fields,result.getLang() ));
    }   
}
