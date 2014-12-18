package com.raythos.sentilexo.bolts.twitter;

/**
 *
 * @author yanni
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.SAXException;
import com.raythos.sentilexo.storm.pmml.NaiveBayesHandler;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class NaiveBayesPMMLBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private static OutputCollector collector;
    static Calendar cal;
    static PrintWriter outputFileWriter;
    static FileWriter file;
    static long counter = 0;
    private static String startTime;
    final static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
    // cluster inputs
    private static String pmmlModelFile = "~/naive_bayes.pmml";
    private static String targetVariable = "Class";
    private static String classificationOutputFile = "~/PredictionResults.txt";
    private static Map<String, Float> prior = new HashMap<>();
    private static Map<String, Float> prob_map = new HashMap<>();
    private static List<String> predictors;
    private static Set<String> possibleTargets;
    NaiveBayesHandler hndlr = new NaiveBayesHandler();

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        try {
            file = new FileWriter(classificationOutputFile + (int) (Math.random() * 100));
            outputFileWriter = new PrintWriter(file);
            cal = Calendar.getInstance();
            cal.getTime();
            String handlerCreationStartTime = sdf.format(cal.getTime());
            // create parser and Naive Bayes handler object which would be used for predicting
            SAXParserFactory spf = SAXParserFactory.newInstance();
            SAXParser parser = spf.newSAXParser();
            parser.parse(new File(pmmlModelFile), hndlr);
            // create local and final variables for use in the map function
            prior = hndlr.prior;
            prob_map = hndlr.prob_map;
            predictors = hndlr.predictors;
            possibleTargets = hndlr.possibleTargets;
            // record the start time of the process of populating the handler object
            cal = Calendar.getInstance();
            cal.getTime();
            String handlerCreationEndTime = sdf.format(cal.
                    getTime());
            outputFileWriter.println("The time taken for initializing the Naive Bayes Handler is: "
                    + getTimeDifference(handlerCreationStartTime, handlerCreationEndTime));
            outputFileWriter.flush();
            cal = Calendar.getInstance();
            cal.getTime();
            startTime = sdf.format(cal.getTime());
        } catch (IOException e) {
            // log.
        } catch (ParserConfigurationException | SAXException | ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String inputRecord = input.getString(0);
        String actualCategory = "", entryList = "";
        // make sure that the record is not empty AND it is notthe first line of the input file listing the names of target and predictor variables
        if (!inputRecord.isEmpty() && !inputRecord.contains(targetVariable)) {
            // split the input string on any of these: [ \\t\\n\\x0B\\f\\r]
            String[] recordEntries = inputRecord.split("\\s+");
            actualCategory = recordEntries[0];
            // remove the first entry of the line representing the target variable value
            recordEntries = (String[]) Arrays.copyOfRange(recordEntries, 1, recordEntries.length);
            for (String entry : recordEntries) {
                entryList += (entry.trim() + ",");
            }
        }
        
        String predictedValue = null;
        if (!actualCategory.isEmpty()) {
            counter++;
            outputFileWriter.append(" Actual Category: " + actualCategory);
            predictedValue = hndlr.predictItNow(entryList, prior, predictors, prob_map, possibleTargets);
            // calculate the current time for logging
            cal = Calendar.getInstance();
            cal.getTime();
            String endTime = sdf.format(cal.getTime());
            outputFileWriter.append("Predicted Category: "
                    + predictedValue + " Start Time: " + startTime + " Current Time: " + endTime);
            outputFileWriter.flush();
        }
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    private static BufferedReader open(String inputFile) throws IOException {
        InputStream in;
        try {
            in = Resources.getResource(inputFile).openStream();
        } catch (IllegalArgumentException e) {
            in = new FileInputStream(new File(inputFile));
        }
        return new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
    }
    // calculates the difference in two given timings

    public static long getTimeDifference(String time1, String time2) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
        Date t1 = formatter.parse(time1);
        Date t2 = formatter.parse(time2);
        long difference = (t2.getTime() - t1.getTime()) / 1000;
        return difference;
    }

}
