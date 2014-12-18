
package com.raythos.sentilexo.storm.pmml;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author yanni
 */
public class NaiveBayesPMMLModelLoader {
     protected  static Logger log = LoggerFactory.getLogger(NaiveBayesPMMLModelLoader.class);
    
    public static NaiveBayesHandler  loadModel(String pmmlModelFile){ 
       NaiveBayesHandler handler = new NaiveBayesHandler();
       try
       {
        log.trace("instantiating SAX Parser");
        // create parser and Naive Bayes handler object which would be used for predicting
        SAXParserFactory spf = SAXParserFactory. newInstance();        
        SAXParser parser = spf.newSAXParser();
        log.trace("pasring model from file " + pmmlModelFile);
        parser.parse(new File(pmmlModelFile), handler);
        log.trace("model loaded from file " + pmmlModelFile);

        // create local and final variables for use in the map function
        }  catch (IOException e) {
            log.error("Error loading model file "  + pmmlModelFile,e );
        } catch (ParserConfigurationException e) {
            log.error("Parser config error when parsing model file "  + pmmlModelFile,e );
        } catch (SAXException e) {
            log.error("SAX  error when parsing model file "  + pmmlModelFile,e );
        }    
       return handler;
}
}
