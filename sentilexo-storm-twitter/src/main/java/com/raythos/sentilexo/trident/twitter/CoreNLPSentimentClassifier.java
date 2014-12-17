
package com.raythos.sentilexo.trident.twitter;

import java.io.Serializable;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.util.Properties;

/**
 *
 * @author yanni
 */

public class CoreNLPSentimentClassifier implements Serializable {
    static CoreNLPSentimentClassifier instance;
   
   
    private StanfordCoreNLP pipeline;
    
    public void setup() {
          Properties props = new Properties();
          props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
          pipeline = new StanfordCoreNLP(props);
    }
    
    public StanfordCoreNLP getPipeline(){
        return pipeline;
    }
    
    public static CoreNLPSentimentClassifier getInstance() {
        if (instance==null) {
            instance = new CoreNLPSentimentClassifier();
        }
        return instance;
    }
}
