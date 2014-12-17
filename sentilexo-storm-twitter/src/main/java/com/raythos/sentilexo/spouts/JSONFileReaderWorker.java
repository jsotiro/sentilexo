/*
 */
package com.raythos.sentilexo.spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 *
 * @author yanni
 */
public class JSONFileReaderWorker implements Runnable, Serializable {
    private int batchSize;
    private int bufferSize;
    private String basePath;     
    private boolean finished = false;
    private File[] files;
    private File currentFile = null;
    private int currentFileIdx = 0;
    private final String pathSep = System.getProperty("file.separator");
    
    
    private FileInputStream fis = null;
    private InputStreamReader isr = null;
    private BufferedReader br = null;
   
    private int linesRead  =0;
    private int statusesRead  =0;
    private int filesRead  =0;
   
    private String filename;
    private String  queryName; 
    private String queryTerms;
    private  ArrayList<String> buffer = new ArrayList<>();
    
    protected  static Logger log = LoggerFactory.getLogger(JSONFileReaderWorker.class);
 
    
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
    

    protected  Status getStatusFromRawJsonLine(String rawJSONLine) {
        int lineNo = this.getLinesRead() ;  
        Status status=null;
          try {
             log.trace("processing File + "+ filename +" - line #"+lineNo);
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
    
      
    public void startReadingFiles() throws IOException{
     
          if (finished) return;
          openNextFile();
          try
           {
            openStreams();
            readNextBacthOfFileLines();
           } 
           catch (IOException ioe) {
            log.error("Failed to read tweets: " + ioe.getMessage());
            finished = true;
            
       }   
        
    }
    
      void endOfJob(){
         log.trace(" Files read :"+ getFilesRead());
         log.trace(" Lines read :"+getLinesRead());
         finished = true;
 
      }
    
      
      void openNextFile() throws IOException{
          currentFile = files[currentFileIdx];
          filename = getCurrentFile().getName();         
          log.trace("Reading file "+currentFileIdx + "  " + currentFile.getName() );
          openStreams();  
      }
      
      void endOfFile() throws IOException{
       closeStreams();
       File newNameFile = new File(basePath + pathSep+currentFile.getName()+"-DONE");
       currentFile.renameTo(newNameFile); 
       filesRead++;
       currentFileIdx++;
       if (currentFileIdx>=files.length)
         endOfJob();
       else {
           openNextFile();
       
        }
   
    }
    
    
    public  int scanForFilesFromPath() {
       
       linesRead  =0;
       statusesRead  =0;
       filesRead = 0;
       finished = false;
       currentFileIdx = 0;
      
            File dir = new File(basePath);
            files = dir.listFiles(new FilenameFilter() {
            @Override
              public boolean accept(File dir, String name) {
                    return name.endsWith(".json");
                }
            });
            System.out.println("Base path is "+basePath);
            int filesToRead=0;
            if (files!=null) {
                filesToRead = files.length; 
            }
            finished = filesToRead < 1; 
            System.out.println("Containing "+filesToRead+ "  files");
            return filesToRead;
       
 
    }

             
            
             
    
    public  void readNextBacthOfFileLines() throws IOException {
            String rawJSONLine;
            while ((rawJSONLine = br.readLine()) != null) {
               linesRead++;
               log.trace("Reading line "+filesRead + linesRead+" of file  " + currentFile.getName() );  
               buffer.add(rawJSONLine);
               if (buffer.size()>=bufferSize) {
                 log.trace("max bufferSize reached - paused till data is emmited to read next batch");
                  return;   
                }  
               }
            if (rawJSONLine==null)
                endOfFile();
            
         }
           

  void openStreams() throws IOException {
         fis = new FileInputStream(currentFile);
         isr = new InputStreamReader(fis, "UTF-8");
        br = new BufferedReader(isr);  
  }  
  void closeStreams() {
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
    
    
     public boolean isFinished() {
       return this.finished;    
    }

    public boolean hasNoMoreData() {
       return (finished && (buffer.isEmpty())); 
    }

    @Override
    public void run() {
    //  importFilesFromPath();
    }
    
}
