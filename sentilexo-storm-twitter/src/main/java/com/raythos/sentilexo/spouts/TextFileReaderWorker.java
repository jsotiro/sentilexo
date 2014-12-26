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

/**
 *
 * @author yanni
 */
public class TextFileReaderWorker implements Runnable, Serializable {

    private int batchSize;
    private int bufferSize;
    private String basePath;
    private boolean finished = false;
    private File[] files;
    private File currentFile = null;
    private int currentFileIdx = 0;
    private final String pathSep = System.getProperty("file.separator");
    private String fileExt = "";
    private FileInputStream fis = null;
    private InputStreamReader isr = null;
    private BufferedReader br = null;

    private int linesRead = 0;
   
    private int filesRead = 0;

    private String filename;
    private String queryName;
    private String queryTerms;
    private ArrayList<String> buffer = new ArrayList<>();

    protected static Logger log = LoggerFactory.getLogger(TextFileReaderWorker.class);

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

    public String getFileExt() {
        return fileExt;
    }

    public void setFileExt(String fileExt) {
        this.fileExt = fileExt;
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

  
    public void startReadingFiles() throws IOException {

        if (finished) {
            return;
        }
        openNextFile();
        try {
            openStreams();
            readNextBacthOfFileLines();
        } catch (IOException ioe) {
            log.error("Failed to read lines: " + ioe.getMessage());
            finished = true;

        }

    }

    void endOfJob() {
        log.trace(" Files read :" + getFilesRead());
        log.trace(" Lines read :" + getLinesRead());
        finished = true;

    }

    void openNextFile() throws IOException {
        currentFile = files[currentFileIdx];
        filename = getCurrentFile().getName();
        log.trace("Reading file " + currentFileIdx + "  " + currentFile.getName());
        openStreams();
    }

    void endOfFile() throws IOException {
        closeStreams();
        File newNameFile = new File(basePath + pathSep + currentFile.getName() + "-DONE");
        currentFile.renameTo(newNameFile);
        filesRead++;
        currentFileIdx++;
        if (currentFileIdx >= files.length) {
            endOfJob();
        } else {
            openNextFile();

        }

    }

    public int scanForFilesFromPath() {

        linesRead = 0;
        filesRead = 0;
        finished = false;
        currentFileIdx = 0;

        File dir = new File(basePath);
        files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (fileExt==null || fileExt.equals(""))
                    return true;
                else        
                    return name.endsWith(fileExt);
            }
        });
        System.out.println("Base path is " + basePath);
        int filesToRead = 0;
        if (files != null) {
            filesToRead = files.length;
        }
        finished = filesToRead < 1;
        System.out.println("Containing " + filesToRead + "  files");
        return filesToRead;

    }

    public void readNextBacthOfFileLines() throws IOException {
        String fileLine;
        while ((fileLine = br.readLine()) != null) {
            linesRead++;
            log.trace("Reading line " + filesRead + linesRead + " of file  " + currentFile.getName());
            buffer.add(fileLine);
            if (buffer.size() >= bufferSize) {
                log.trace("max bufferSize reached - paused till data is emmited to read next batch");
                return;
            }
        }
        if (fileLine == null) {
            endOfFile();
        }

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
