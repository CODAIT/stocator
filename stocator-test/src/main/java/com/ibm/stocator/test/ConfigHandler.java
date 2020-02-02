/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  (C) Copyright IBM Corp. 2020
 */

package com.ibm.stocator.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConfigHandler implements Serializable {

  private String propertiesFile;
  private Properties prop = null;

  private Properties loadPropoerties(String propertiesFile) {
    prop = new Properties();
    Configuration mConf = new Configuration(true);
    FileSystem fs = null;
    FSDataInputStream in = null;
    try {
      fs = FileSystem.get(URI.create(propertiesFile), mConf);
      System.out.println("Going to open: " + propertiesFile);
      in =  fs.open(new Path(propertiesFile));
      prop.load(in);
      System.out.println("Property file read successful: " + propertiesFile);
      System.out.println(prop.toString());
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return prop;
  }

  public String getProperty(String key) {
    return prop.getProperty(key);
  }

  public String getServiceName() {
    return "service";
  }

  public ConfigHandler(String propFile) {
    propertiesFile = propFile;
    loadPropoerties(propertiesFile);
  }
  
}
