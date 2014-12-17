package com.raythos.sentilexo.spouts;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.raythos.sentilexo.twitter.TwitterQueryResultItemAvro;
import com.raythos.sentilexo.twitter.common.domain.TwitterQueryResultItemMapper;
import com.raythos.sentilexo.utils.AppProperties;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/**
 * A Spout that emits lines from text files in batches.  
 * 
 * Kafka round trip

 * @author yanni
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class TextFileBacthedLinesSpout  implements IBatchSpout   {
    protected  static Logger log = LoggerFactory.getLogger(TextFileBacthedLinesSpout.class);
    protected  JSONFileReaderWorker worker;
        
      
        public JSONFileReaderWorker getFileWorker() {
            return worker;
        }
	
        public TextFileBacthedLinesSpout() throws IOException {
                this(5);
	}

	public TextFileBacthedLinesSpout(int batchSize) throws IOException {
                this.worker = new JSONFileReaderWorker();
                this.worker.setBatchSize(batchSize);
                this.worker.setBufferSize(batchSize *1000);
	}

        void scanPathForFileReads(){
            int filestoProcess = this.worker.scanForFilesFromPath();
            log.trace( filestoProcess + " JSON files scanned for processing");
            try {
             if (filestoProcess > 0)   
              this.worker.startReadingFiles();
            } catch (IOException ex) {
                log.error("Error when starting reading files. The exception was ", ex);
            }
        }
        @Override
	public void open(Map conf, TopologyContext context) {
		// init
            this.worker.setQueryName(AppProperties.getProperty("QueryName"));
            this.worker.setQueryTerms(AppProperties.getProperty("QueryTerms"));
            log.trace("Stream basic properties loaded and configured from "+AppProperties.getPropertiesFile());
            scanPathForFileReads();
	}

        private Values getNextTweet() {
        Values result = null;
        if (worker.getBuffer().size() > 0) {
            String lineToProcess = (String)worker.getBuffer().get(0);
            Status status = worker.getStatusFromRawJsonLine(lineToProcess);
            if (status!=null) {
                byte[] data = getSerialisedStatusObject(status);
                result = new Values(data);
               
            }
           }            
        return result;
        }
        
	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// emit batchSize 
                Values result=null;
		for(int i = 0; i < this.worker.getBuffer().size(); i++) {
			result = getNextTweet();
                        worker.getBuffer().remove(0);
                        if (result!=null)
                            collector.emit(result);
		}
                
                 if ( worker.isFinished() ) {
                     scanPathForFileReads();
                 }
                 else
                    try { 
                         worker.readNextBacthOfFileLines();
                        } catch (IOException ex) {
                        log.error("error reading next batch. The exception was ", ex);
                        }

	}

	@Override
	public void ack(long batchId) {
		// nothing to do here
	}

	@Override
	public void close() {
		// nothing to do here
	}

	@Override
	public Map getComponentConfiguration() {
		// no particular configuration here
		return new Config();
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("bytes");
	}

	
       
        byte[] getSerialisedStatusObject(Status status){        
        byte[] data  = null; 
        log.trace("Posting Status with id " +status.getId() +" from File " + this.worker.getFilename());
                try{
                    TwitterQueryResultItemAvro tqri = new TwitterQueryResultItemAvro();
                    tqri = TwitterQueryResultItemMapper.mapItem(this.worker.getQueryName(), this.worker.getQueryTerms(), status);
                    data = TwitterQueryResultItemMapper.getAvroSerialized(tqri);
                    log.trace("AVRO serialised Status with id " +status.getId() +" was obtained from file" + this.worker.getFilename());
                                        } 
                catch (Exception e) {
                log.error("Error when emmitting bytes. The exception was "+e);
                }          
              return data;  
        } 
        
        
	public static void main(String[] args) throws IOException, ParseException {
            TextFileBacthedLinesSpout spout = new TextFileBacthedLinesSpout(5);
            String testBasePath = "/Users/yanni/sentidata";
            try {
                spout.getFileWorker().setBasePath(testBasePath);
                spout.open(null, null);
                while (!spout.getFileWorker().hasNoMoreData()) {
                   spout.getNextTweet();
                }
                log.trace("Status items read : "+ spout.getFileWorker().getStatusesRead());
                System.exit(0);
        } catch (Exception e) {

            log.error("exception with error: " + e.getMessage());
            System.exit(-10);
       }  
    }

   
    
}


