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

package com.raythos.sentilexo.twitter.domain;

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
