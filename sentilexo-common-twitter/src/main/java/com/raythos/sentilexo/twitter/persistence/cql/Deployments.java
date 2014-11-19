/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.raythos.sentilexo.twitter.persistence.cql;

/**
 *
 * @author yanni
 */
public class Deployments {
  private long deploymentNo = 0L;
  private static Deployments instance;
  
  private Deployments(){
     super(); 
  }
  public static Deployments getInstance(){
      if (instance==null) 
           instance = new Deployments();
      return instance;
  }

    public long getDeploymentNo() {
        return deploymentNo;
    }

    public void setDeploymentNo(long deploymentNo) {
        this.deploymentNo = deploymentNo;
    }
  
    public void load(){}
    
    public void save(){}
  
}
