package com.raythos.sentilexo.utils;

/**
 *
 * @author yanni
 */

import java.io.FileInputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppProperties
{
Logger log = LoggerFactory.getLogger(AppProperties.class); 
    private static AppProperties instance;
    private  String propertiesFile = "/etc/raythos/sentilexo.properties";
    private Properties props;
    private final ClassLoader classLoader;
    
    public static String getPropertiesFile() {
        return getInstance().propertiesFile;
    }

    public static void setPropertiesFile(String propertiesFile) {
        getInstance().propertiesFile = propertiesFile;
    }

    private void load(){
         try
        {
            this.props = new Properties();
                props.load(classLoader.getResourceAsStream(propertiesFile));
        }
        catch (Exception ex)
        {
            log.error("Error when loading configuration", ex);
        }
    
    }
 
    
    public void loadAsFile(){
         try
        {
            this.props = new Properties();
                props.load(new FileInputStream(propertiesFile));
        }
        catch (Exception ex)
        {
            log.error("Error when loading configuration", ex);
        }
     }
    
    
    public static void Reload(){
        getInstance().load();
    }
    
    private AppProperties()
    {    classLoader = ClassLoader.getSystemClassLoader();
        loadAsFile();
     }

    private static AppProperties getInstance()
    {
        if (instance == null)
            instance = new AppProperties();
        return instance;
    }

    public static String getProperty(String key)
    {
        return getInstance().props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue)
    {
        return getInstance().props.getProperty(key, defaultValue);

    }
   
}