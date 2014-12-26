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

import java.util.HashMap;

/**
 *
 * @author yanni
 */
public class Deployments {
  static HashMap<String,Deployment> deployments = new HashMap();    

    public static Deployment getInstance(String topologyName) {
        Deployment depl;
        String topology = topologyName.toUpperCase();
        if(deployments.containsKey(topology)){
            depl = deployments.get(topology);
        }
        else depl = new Deployment(topology,0);
        return depl;
    }
}
