/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author yanni
 */
public class DateTimeUtils {
  public static final int  HOUR  = 0;
  public static final int  DAY =  1;    
  public static final int  WEEK = 2;
  public static final int MONTH = 3;
  public static final int YEAR = 4;
  public static final int ALL = -1;

  static Logger log = LoggerFactory.getLogger(DateTimeUtils.class); 

  
  public static SimpleDateFormat[] sdfs = {new SimpleDateFormat("yyyy-MM-dd-HH"),
                             new SimpleDateFormat("yyyy-MM-dd"),
                             new SimpleDateFormat("yyyy-w"),
                             new SimpleDateFormat("yyyy-MM"),
                             new SimpleDateFormat("yyyy")};
   
    public static final String DATE_FORMAT = "yyyy-MM-dd z HH:mm:ss.SSS";
    public static final SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);

  
     
    public static Date getDateFromString(String dateStr){    
        try {
            log.warn("getDateFromStringEx "+dateStr + "with format "+df.toPattern());
            return df.parse((String)dateStr);
        } catch (ParseException ex) {
             log.error("getDateFromStringEx "+dateStr + "with format "+df.toPattern());
           return null;
        }
    }  
   
  public static String getBucket(int bucketType, Date bucketDate){
     String result = "0";
       if (bucketType > -1)
           result = sdfs[bucketType].format(bucketDate);
               
       return result;
    }

  public static String getDateAsText(Date d){
      return df.format(d);
                  

  }
  
   public static Date getDateFromStringEx(String dateStr,SimpleDateFormat dft ){    
        try {
            log.trace("getDateFromStringEx "+dateStr + "with format "+dft.toPattern());
            return dft.parse((String)dateStr);
        } catch (ParseException ex) {
                log.error("Error in getDateFromStringEx "+ex.getMessage());
           return null;
        }
   }
   
    public static Date getBucketDateTime(int bucketType, Date bucketDate){
     Date result=null;
     String dateStr;
      if (bucketType > -1) {
        dateStr = getBucket(bucketType, bucketDate);
        result = getDateFromStringEx(dateStr, sdfs[bucketType]);
       } else
           result = getDateFromStringEx("2000", sdfs[YEAR]);
     
     return result;
  }
    
     public static void initTimezonesToUTC(){
         df.setTimeZone(TimeZone.getTimeZone("UTC"));
         for (SimpleDateFormat sdf : sdfs)
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        
     }
  
    public static void main(String[] args) {
       Date now = new Date();
       System.out.println(getBucket(HOUR, now)+"   " + getBucketDateTime(HOUR, now));  
       System.out.println(getBucket(DAY, now)+"   " +getBucketDateTime(DAY, now));  
       System.out.println(getBucket(WEEK, now)+"   " +getBucketDateTime(WEEK, now));  
       System.out.println(getBucket(MONTH, now)+"   " +getBucketDateTime(MONTH, now));
       System.out.println(getBucket(YEAR, now)+"   " +getBucketDateTime(YEAR, now));
       System.out.println(getBucket(ALL, now)+"   " +getBucketDateTime(ALL, now));
        
    }     
  
}
