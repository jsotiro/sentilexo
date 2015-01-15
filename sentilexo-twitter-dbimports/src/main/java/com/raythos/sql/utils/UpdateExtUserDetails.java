/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sql.utils;

import com.raythos.sentilexo.common.utils.AppProperties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public class UpdateExtUserDetails {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            AppProperties.setPropertiesFile("/etc/raythos/dbutils.properties");
            UpdateExtUserDetailsWorker updater = new  UpdateExtUserDetailsWorker();
                updater.connect(AppProperties.getProperty("dbhost"),
                                AppProperties.getProperty("db"), 
                                AppProperties.getProperty("dbuser"), 
                                AppProperties.getProperty("dbpwd"));
            updater.updateTable();
        } catch (Exception ex) {
            Logger.getLogger(UpdateExtUserDetails.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
}
