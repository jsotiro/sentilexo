/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sentilexo.files.twitter;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author yanni
 * 
 * based to an Twitter4J example by
 * Yusuke Yamamoto - yusuke at mac.com
 * 
 */
public class TwitterJSONLoader {
   static Logger log = LoggerFactory.getLogger(TwitterJSONLoader.class);
   int linesRead  =0;
   int statusesRead  =0;
   int filesRead  =0;
   File currentFile;
   String filename;
   protected String  queryName; 
   protected String queryTerms;
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

    public static void setLog(Logger log) {
        TwitterJSONLoader.log = log;
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
    
   
   
 
public  int importFilesFromPath(String basePath) {
       
       linesRead  =0;
       statusesRead  =0;
       filesRead = 0;
   
       try {
            File dir = new File(basePath);
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".json");
                }
            });
            System.out.println("Base path is "+basePath);
            if (files==null)
               return -1; 
            System.out.println("Containing "+files.length+ "  files");
            for (File file : files) {
                currentFile = file;
                filename = getCurrentFile().getName();
                filesRead++;
                log.trace("Reading file "+filesRead + "  " + file.getName() );
                readFileLines(file);
                File newNameFile = new File(file.getName()+"-DONE");
                file.renameTo(newNameFile);
             
            }
            
            return 0;
        } catch (IOException ioe) {

            log.error("Failed to store tweets: " + ioe.getMessage());
            return -2;
       } 
 
    }

    
        void handleStatusObject(int lineNo, Status status, String rawJSONLine){
         //override   
        }
    protected  void handleRawJsonLine(String rawJSONLine) {
        int lineNo = this.getLinesRead() ;  
        Status status;
          try {
             log.trace("processing File + "+ filename +"line #"+lineNo);
            if (rawJSONLine.startsWith("{\"created_at")){
                status = TwitterObjectFactory.createStatus(rawJSONLine);
                handleStatusObject(lineNo, status,rawJSONLine);
                log.trace("processing File + "+ filename +"line #"+lineNo + "containes twitter status");
                statusesRead++;
     
              }
            else 
                log.warn("File "+ filename+  " line "+lineNo +  " has no twitter status JSON text");
        } catch (Exception ex) {
            log.error("Exception was raised: " + ex);
        } 
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
               handleRawJsonLine(rawJSONLine);            
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

    private void handleStatusObject() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
