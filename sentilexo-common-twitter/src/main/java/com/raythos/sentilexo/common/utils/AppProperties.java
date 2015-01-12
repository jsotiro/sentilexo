/*
 * Copyright 2014 (c) Raythos Interactive Ltd  http://www.raythos.com
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
package com.raythos.sentilexo.common.utils;

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