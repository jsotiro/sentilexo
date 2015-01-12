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

package com.raythos.sentilexo.trident.twitter.sentiment;

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
