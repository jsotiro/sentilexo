/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.raythos.sql.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author yanni
 */
public class CopyHashtagTable {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)  {
        // TODO code application logic here
        TableCopyWorker tblCopier = new TableCopyWorker();
        tblCopier.setSourceTable("Hashtags");
        tblCopier.setDestTable("StatusHashtags");
        
        
        try {
            tblCopier.connect("localhost:3306","TwitterStore", "raythos", "Macedonia1");
            long id = tblCopier.getTargetStartId();
            tblCopier.setLastId(id);
            tblCopier.copyTable();
        } catch (Exception ex) {
                Logger.getLogger(CopyHashtagTable.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }
    
}
