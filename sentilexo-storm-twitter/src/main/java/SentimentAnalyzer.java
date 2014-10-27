

import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalyzer {
      static StanfordCoreNLP pipeline = null;
    
    public static void  initialiseCoreNLP(){
       Properties props = new Properties();
       props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
       pipeline = new StanfordCoreNLP(props);         
    }
    
    public static int sentimentScore(String line) {
        
        int mainSentiment = 0;
        if (line != null && line.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;

    }


    public static void main(String[] args) {
        SentimentAnalyzer.initialiseCoreNLP();
        int sentiment = SentimentAnalyzer.sentimentScore("very optimistic today");
        System.out.println(sentiment);
        sentiment = SentimentAnalyzer.sentimentScore("I think i should be");
        System.out.println(sentiment);
        sentiment = SentimentAnalyzer.sentimentScore("het lost!");
        System.out.println(sentiment);
        
        
    }
}