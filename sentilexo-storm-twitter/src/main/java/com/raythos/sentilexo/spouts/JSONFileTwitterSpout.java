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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 * A Spout that emits fake tweets from json files. useful in testing topologies without having to go through the whole
 * Kafka round trip

 * @author yanni
 */
@SuppressWarnings({ "serial", "rawtypes" })
public class JSONFileTwitterSpout  implements IBatchSpout   {
    protected  static Logger log = LoggerFactory.getLogger(JSONFileTwitterSpout.class);
    private int batchSize;
    private int bufferSize;
    private String basePath;     
    private boolean finished = false;
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public ArrayList<String> getBuffer() {
        return buffer;
    }

    public void setBuffer(ArrayList<String> buffer) {
        this.buffer = buffer;
    }
    private int linesRead  =0;
    private int statusesRead  =0;
    private int filesRead  =0;
    private File currentFile;
    private String filename;
    private String  queryName; 
    private String queryTerms;
    private  ArrayList<String> buffer = new ArrayList<>();

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getQueryTerms() {
        return queryTerms;
    }

    public void setQueryTerms(String queryTerms) {
        this.queryTerms = queryTerms;
    }

    public int getLinesRead() {
        return linesRead;
    }

    public void setLinesRead(int linesRead) {
        this.linesRead = linesRead;
    }

    public int getStatusesRead() {
        return statusesRead;
    }

    public void setStatusesRead(int statusesRead) {
        this.statusesRead = statusesRead;
    }

    public int getFilesRead() {
        return filesRead;
    }

    public void setFilesRead(int filesRead) {
        this.filesRead = filesRead;
    }

    public static Logger getLog() {
        return log;
    }


   
    
    public File getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(File currentFile) {
        this.currentFile = currentFile;
    }
   
    public String getFilename() {
        return filename;
    }
    
public  int importFilesFromPath() {
       
       linesRead  =0;
       statusesRead  =0;
       filesRead = 0;
       finished = false;
       try {
           
            File dir = new File(basePath);
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".json");
                }
            });
            System.out.println("Base path is "+basePath);
            if (files==null) {
                finished = true;
                return -1; 
            }
            System.out.println("Containing "+files.length+ "  files");
            for (File file : files) {
                currentFile = file;
                filename = getCurrentFile().getName();
                filesRead++;
                log.trace("Reading file "+filesRead + "  " + file.getName() );
                readFileLines(file);
                //File newNameFile = new File(file.getName()+"-DONE");
                //file.renameTo(newNameFile);         
            }
               log.trace(" Files read :"+ getFilesRead());
               log.trace(" Lines read :"+getLinesRead());
          
            finished = true;
            return 0;
        } catch (IOException ioe) {

            log.error("Failed to store tweets: " + ioe.getMessage());
            finished = true;
            return -2;
       } 
 
    }

    protected  Status getStatusFromRawJsonLine(String rawJSONLine) {
        int lineNo = this.getLinesRead() ;  
        Status status=null;
          try {
             log.trace("processing File + "+ filename +"line #"+lineNo);
            if (rawJSONLine.startsWith("{\"created_at")){
                status = TwitterObjectFactory.createStatus(rawJSONLine);    
                statusesRead++;
                log.trace("File + "+ filename +"line #"+lineNo + "containes twitter status. So far "+statusesRead+" Status objects read");

              }
            else 
                log.warn("File "+ filename+  " line "+lineNo +  " has no twitter status JSON text");
        } catch (Exception ex) {
            log.error("Exception was raised: " + ex);
        }
     return status;   
    }
    
    private  void readFileLines(File fileName) throws IOException {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(fileName);
            isr = new InputStreamReader(fis, "UTF-8");
            br = new BufferedReader(isr);
         
            String rawJSONLine;
            while ((rawJSONLine = br.readLine()) != null) {
               linesRead++;
               log.trace("Reading line "+filesRead + linesRead+" of file  " + fileName.getName() );  
               buffer.add(rawJSONLine);
               while ( buffer.size()>=bufferSize ) {
                  log.trace("waiting to read next batch");
                   //wait  
                }
            
            }
        }    
        finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ignore) {
                }
            }
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException ignore) {
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
        
        private Values getNextTweet() {
        Values result = null;
        if (buffer.size() > 0) {
            String lineToProcess = (String)buffer.get(0);
            Status status = getStatusFromRawJsonLine(lineToProcess);
            if (status!=null) {
                byte[] data = getSerialisedStatusObject(status);
                result = new Values(data);
                buffer.remove(0);
            }
           }            
        return result;
        }

	public JSONFileTwitterSpout() throws IOException {
		this(5);
	}

	public JSONFileTwitterSpout(int batchSize) throws IOException {
		this.batchSize = batchSize;
                this.bufferSize = batchSize *1000;
	}


        @Override
	public void open(Map conf, TopologyContext context) {
		// init
            setQueryName(AppProperties.getProperty("QueryName"));
            setQueryTerms(AppProperties.getProperty("QueryTerms"));
            log.trace("Stream basic properties loaded and configured from "+AppProperties.getPropertiesFile());
            importFilesFromPath();            
	}

        
        
	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		// emit batchSize fake tweets
                Values result=null;
		for(int i = 0; i < batchSize; i++) {
			result = getNextTweet();
                        if (result!=null)
                            collector.emit(result);
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
        int lineNo = this.getLinesRead();
        byte[] data  = null; 
        log.trace("Posting Status with id " +status.getId() +"from File" + getFilename()+" line #"+ lineNo);
                try{
                    TwitterQueryResultItemAvro tqri = new TwitterQueryResultItemAvro();
                    tqri = TwitterQueryResultItemMapper.mapItem(queryName, queryTerms, status);
                    data = TwitterQueryResultItemMapper.getAvroSerialized(tqri);
                    log.trace("AVRO serialised Status with id " +status.getId() +" was obtained from file" + getFilename() +" line #"+lineNo);
                                        } 
                catch (Exception e) {
                log.error("error when emmitting bytes"+e);
                }          
              return data;  
        } 
        
        
	public static void main(String[] args) throws IOException, ParseException {
            JSONFileTwitterSpout spout = new JSONFileTwitterSpout(5);
            String testBasePath = "/Users/yanni/sentidata";
            try {
                spout.setBasePath(testBasePath);
                spout.open(null, null);
                while (!spout.hasNoMoreData()) {
                   spout.getNextTweet();
                }
                log.trace("Status items read :"+ spout.getStatusesRead());
                System.exit(0);
        } catch (Exception e) {

            log.error("exception with error: " + e.getMessage());
            System.exit(-10);
       }  
    }

    private boolean isFinished() {
       return finished;    
    }

    private boolean hasNoMoreData() {
       return (finished && (buffer.isEmpty())); 
    }

    
}

