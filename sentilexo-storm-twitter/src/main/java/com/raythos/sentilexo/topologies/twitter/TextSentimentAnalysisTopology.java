/*
 * Copyright 2015 (c) Raythos Interactive Ltd. - http://www.raythos.com
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
package com.raythos.sentilexo.topologies.twitter;

import backtype.storm.tuple.Fields;
import com.raythos.sentilexo.common.utils.AppProperties;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;
import com.raythos.sentilexo.trident.twitter.sentiment.CalculateNLPSentiment;
import com.raythos.sentilexo.trident.twitter.sentiment.CoreNLPSentimentClassifier;
import com.raythos.sentilexo.trident.twitter.sentiment.DirectCalculatePmmlBayesSentiment;
import java.util.Properties;
import storm.trident.Stream;

/**
 *
 * @author John Sotiropoulos
 */
public class TextSentimentAnalysisTopology extends BaseStreamAnalyticsTopology {
    
    
    @Override
    public void defineFunctions() {
          // neural network based sentiment calculation
          Properties props = new Properties();          
          // initialise the CoreNLP engine - this takes time do it before the topology is submited.
          CoreNLPSentimentClassifier.getInstance().setup();
          CalculateNLPSentiment neuralNetNLPSentimentAnalysisFunction = new CalculateNLPSentiment();
          
          // setup for the Naive Bayes classification model developed in R or RapidMiner and exported using PMML 
          String pmmlFileName = AppProperties.getProperty("bayes-model", "twitter-sentiment-bayes.xml");
          NaiveBayesHandler handler= null;//NaiveBayesPMMLModelLoader.loadModel(pmmlFileName);
          DirectCalculatePmmlBayesSentiment bayesModelClassifier = new DirectCalculatePmmlBayesSentiment(handler);
         
          /*try {
             PmmlModel model = new PmmlModel(pmmlFileName);
             PmmlModelEvaluationFunction calcPmml = new PmmlModelEvaluationFunction();
             calcPmml.setEvaluator(model.getModelEvaluator());
             
         } catch (Exception ex) {
            log.error("Error loading PMML model " + pmmlFileName + " .The error was", ex);
         }
           */   

            Stream nlpSentimentStream = getMainStream()
                   .each(itemField,neuralNetNLPSentimentAnalysisFunction ,new Fields("NLPSentiment"));
            Stream bayesSentimentStream = getMainStream()
            .each(itemField, bayesModelClassifier , new Fields("BayesSentiment"));
     
    }    

     
     public static void main(String[] args)  {
        TextSentimentAnalysisTopology text_analysis_topology = new  TextSentimentAnalysisTopology();
        text_analysis_topology.configFromArgs(args);
        text_analysis_topology.execute();        
    } 
    
}
