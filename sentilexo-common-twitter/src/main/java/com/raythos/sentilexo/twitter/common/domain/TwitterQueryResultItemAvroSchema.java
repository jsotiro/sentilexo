package com.raythos.sentilexo.twitter.common.domain;
import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

public class TwitterQueryResultItemAvroSchema {
    
    public static void main(String[] args) {
  
    Schema schema = ReflectData.get().getSchema(TwitterQueryResultItem.class);
    System.out.println(schema);
    }   
    
}
