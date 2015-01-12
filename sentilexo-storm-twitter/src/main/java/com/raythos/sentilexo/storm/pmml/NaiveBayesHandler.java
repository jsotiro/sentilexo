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

/*
 * Code based and adapted from material in 
 * Big Data Analytics Beyond Hadoop
 * by Vijay Srinivas Agneeswaran, Ph.D.
 *
 */
package com.raythos.sentilexo.storm.pmml;

import com.raythos.sentilexo.trident.twitter.sentiment.DirectCalculatePmmlBayesSentiment;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class NaiveBayesHandler extends DefaultHandler implements Serializable {

    protected static Logger log = LoggerFactory.getLogger(NaiveBayesHandler.class);

    boolean miningSchema = false;
    boolean miningField = false;
    boolean bayesInputs = false;
    boolean bayesInput = false;
    boolean pairCount = false;
    boolean tvCounts = false;
    boolean tvCount = false;
    boolean forPrior = true;
    public static String bi_fn = "";
    public static String pc_val = "";
    public static int bayesInputIndex = 0;
    public static String targetVar = "";
    public static List<String> predictors = new ArrayList<>();
    public static Set<String> possibleTargets = new HashSet<>();
    public static int sum = 0;
    public static Map<String, Float> stats = new HashMap<>();
    public static Map<String, Float> prior = new HashMap<>();
    public static Map<String, Float> prob_map = new HashMap<>();
    public static Map<String, Map<String, Map<String, Float>>> classMap = new HashMap<>();

    public final static class TargetValueCounts implements Serializable {

        Map<String, Integer> tc_map;
    }

    public final static class PairCounts implements Serializable {

        List<TargetValueCounts> pc = new ArrayList<>();
    }

    public final static class BayesInput implements Serializable {

        Map<String, PairCounts> pCounts_map;
    }

    public final static class BayesInputs implements Serializable {

        static Map<String, BayesInput> bi_map;

        public static String createMap() {
            String ret = "";
            Set<String> set = bi_map.keySet();
            Iterator<String> s = set.iterator();
            while (s.hasNext()) {
                String fieldName = s.next();
                Set<String> set1 = bi_map.get(fieldName).pCounts_map.keySet();
                Iterator<String> s1 = set1.iterator();
                while (s1.hasNext()) {
                    String valueOfField = s1.next();
                    TargetValueCounts var = bi_map.get(fieldName).pCounts_map.get(valueOfField).pc.get(0);
                    Set<String> set2 = var.tc_map.keySet();
                    Iterator<String> s2 = set2.iterator();
                    while (s2.hasNext()) {
                        String class1 = s2.next();
                        Float divisor = stats.get(class1);
                        if (divisor == null) {
                            divisor = 1.0F;
                        }
                        prob_map.put((class1 + "_" + valueOfField + "_" + fieldName), (var.tc_map.get(class1).floatValue() + 1) / divisor);
                    }
                }
            }
            return ret;
        }
    }

    BayesInputs bis;

    @Override

    public void startDocument() throws SAXException {
    }

    @Override
    public void endDocument() throws SAXException {
        Set<String> keys = stats.keySet();
        Iterator<String> itr = keys.iterator();

        while (itr.hasNext()) {
            String s = itr.next();
            sum += stats.get(s);
        }
        itr = keys.iterator();
        // compute prior
        while (itr.hasNext()) {
            String s = itr.next();
            prior.put(s, (stats.get(s) / sum));
        }
        // print prior
        itr = prior.keySet().iterator();
        while (itr.hasNext()) {
            String p = itr.next();
        }
        BayesInputs.createMap();
    }

    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) throws SAXException {
        if (qName == "MiningSchema") {
            miningSchema = true;
        } else if (miningSchema = true && qName == "MiningField") {
            for (int i = 0; i < attributes.getLength(); i++) {
                if (attributes.getValue(i).compareTo("predicted") == 0) {
                    targetVar = attributes.getValue("name");
                    log.debug("Target=" + targetVar);
                } else if (attributes.getValue(i).compareTo("active") == 0) {
                    predictors.add(attributes.getValue("name"));
                }
            }
            miningField = true;
        } else if (qName.compareTo("BayesInputs") == 0) {
            bayesInputs = true;
            bis = new BayesInputs();
            bis.bi_map = new HashMap<String, NaiveBayesHandler.BayesInput>();
        } else if (bayesInputs == true && qName.compareTo("BayesInput") == 0) {
            bayesInput = true;
            BayesInput bi = new BayesInput();
            bis.bi_map.put(attributes.getValue(0), bi);
            bi_fn = attributes.getValue(0);
            bis.bi_map.get(bi_fn).pCounts_map = new HashMap<>();
        } else if (bayesInput == true && qName.compareTo("PairCounts") == 0) {
            pairCount = true;
            PairCounts pc = new PairCounts();
            bis.bi_map.get(bi_fn).pCounts_map.put(attributes.getValue(0), pc);
            pc_val = attributes.getValue(0);
        } else if (pairCount == true && qName.compareTo("TargetValueCounts") == 0) {
            tvCounts = true;
            TargetValueCounts tvcS = new TargetValueCounts();
            bis.bi_map.get(bi_fn).pCounts_map.get(pc_val).pc.add(tvcS);
            bis.bi_map.get(bi_fn).pCounts_map.get(pc_val).pc.get(0).tc_map = new HashMap<String, Integer>();
        } else if (tvCounts == true && qName.compareTo("TargetValueCount") == 0) {
            tvCount = true;
            String key = attributes.getValue("value");
            Integer value = Integer.parseInt(attributes.
                    getValue("count"));
            bis.bi_map.get(bi_fn).pCounts_map.get(pc_val).pc.get(0).tc_map.put(key, value);
            possibleTargets.add(key);
            if (forPrior == true) {
                if (stats.containsKey(key)) {
                    stats.put(key, stats.get(key) + value);
                } else {
                    stats.put(key, Float.parseFloat(attributes.
                            getValue("count")));
                }
            }
        }
    }

    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        if (qName == "MiningSchema") {
            miningSchema = false;
            /*
             System.out.print(targetVar + "=");
             for (int i = 0; i < predictors.size(); i++) {
             System.out.print(predictors.get(i));
             if (i < predictors.size() - 1) {
             System.out.print("+");
             }
             }
             System.out.println();
             */
        } else if (miningSchema == true && qName == "MiningField") {
            miningField = false;
        } else if (qName.compareTo("BayesInputs") == 0) {
            bayesInputs = false;
        } else if (qName.compareTo("BayesInput") == 0) {
            bayesInput = false;
            forPrior = false;
        } else if (qName.compareTo("PairCounts") == 0) {
            pairCount = false;
        } else if (qName.compareTo("TargetValueCounts") == 0) {
            tvCounts = false;
        } else if (qName.compareTo("TargetValueCount") == 0) {
            tvCount = false;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        if (miningSchema == true && miningField == true) {
        }
        super.characters(ch, start, length);
    }

    public String predictItNow(String input, Map<String, Float> priorArg, List<String> predictorsArg, Map<String, Float> prob_mapArg, Set<String> possibleTargetsArg) {
        log.trace("predictItNow  for String " + input);
        StringTokenizer strT = new StringTokenizer(input, ",");
        Iterator<String> itr = priorArg.keySet().iterator();
        int pred_index = 0;
        float max_prob = 0;
        String determined_target = "";
        Iterator<String> targetIter = possibleTargetsArg.
                iterator();
        boolean printed = false;
        while (targetIter.hasNext() && itr.hasNext()) {
            String targetVar = targetIter.next();
            String p = itr.next();
            // create the key for map
            float prior_prob = priorArg.get(p);
            float prob_for_this_class = 1;
            while (strT.hasMoreElements() && pred_index < predictorsArg.size()) {
                String val = strT.nextElement().toString();
                if (printed == false) {
                }
                String map_variable = targetVar + "_" + val + "_" + predictorsArg.get(pred_index++);
                if (prob_mapArg.get(map_variable) == null) {
                    log.error("At least one of the expected variables are not supplied correctly ... Exiting");
                    //outputFile.println("At least one of the expected variables are not supplied correctly");
                    //System.exit(0);
                }

                prob_for_this_class *= prob_mapArg.get(map_variable);
            }
            prob_for_this_class *= prior_prob;
            if (prob_for_this_class > max_prob) {
                max_prob = prob_for_this_class;
                determined_target = targetVar;
            }
            pred_index = 0;
            strT = new StringTokenizer(input, ",");
            printed = true;
        }
        return determined_target;
    }

}
