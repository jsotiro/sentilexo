/*
* Copyright 2014 (c) Raythos Interactive Ltd. - http://www.raythos.com
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
package com.raythos.sentilexo.topologies.twitter;

/**
 *
 * @author John Sotiropoulos
 */
public class FileBasedDataIngressTopology {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
      DataIngressTopology files_data_topology = new DataIngressTopology();      
      files_data_topology.setUseKafka(false);
      files_data_topology.execute();
        
    }
    
}
