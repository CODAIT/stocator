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

import java.util.Random;

public class NameGenerator {
  
  private ConfigHandler conf;
  private String container;
  private String containerPath;
  private String parquetName;
  private String parquetPath;
  private String jsonPath;
  private String jsonName;
  private String csvName;
  private String csvPath1;
  private String csvPath2;
  private String txtName;
  private String txtPath;
  private String identifier;
  private String dataRes;
  private String dataResPath;
  private Boolean randomContainer;
  private String containerSubDir;
  private String defaultFS = null;
  
  private static final int minBound = 10000;
  
  
  public NameGenerator(ConfigHandler confIn) {
    conf = confIn;
    container = conf.getProperty("object.storage.output.container");
    parquetName = "parqetres";
    jsonName = "jsonres";
    csvName = "csvres";
    txtName = "txtres";
    identifier = conf.getProperty("object.storage.scheme");
    dataRes = "data";
    containerSubDir = conf.getProperty("object.storage.output.container.subdir");
    randomContainer = Boolean.valueOf(conf.getProperty("object.storage.output.container.random.suffix"));
  }

  public void setDefaultFS(boolean isDefaultFS) {
    if (isDefaultFS) {
      containerPath = "";
      generateObjectNames();
    }
  }

  public void generateNewContainer() {
    System.out.println("* Use container : " + container);
    if (!containerSubDir.isEmpty()) {
      containerPath = identifier + "://" + container + "/" + containerSubDir + "/";
    } else {
      containerPath = identifier + "://" + container + "/";
    }
  }
  
  public void generateNewContainer(boolean random) {
    System.out.println("* Use random container : " + randomContainer.toString());
    if (randomContainer.booleanValue() || random) {
        Random rn = new Random();
        container = container + String.valueOf(rn.nextInt(minBound) + minBound) ;
    }
    if (!containerSubDir.isEmpty()) {
      containerPath = identifier + "://" + container +  "." + conf.getServiceName() + "/" + containerSubDir + "/";
    } else {
      containerPath = identifier + "://" + container +  "." + conf.getServiceName() + "/";
    }
    if (defaultFS != null) {
      containerPath = "/";
    }
  }

  public void generateNewContainer(String suffix) {
    container = conf.getProperty("object.storage.output.container") + "list";
    if (randomContainer.booleanValue()) {
        Random rn = new Random();
        container = container + String.valueOf(rn.nextInt(minBound) + minBound) ;
    }
    if (!containerSubDir.isEmpty()) {
      containerPath = identifier + "://" + container +  "." + conf.getServiceName() + "/" + containerSubDir + "/";
    } else {
      containerPath = identifier + "://" + container +  "." + conf.getServiceName() + "/";
    }
    if (defaultFS != null) {
      containerPath = "/";
    }
  }

  public void generateObjectNames() {
    parquetPath = containerPath + parquetName;
    dataResPath = containerPath + dataRes;
    jsonPath = containerPath + jsonName;
    csvPath1 = containerPath + csvName;
    csvPath2 = containerPath + "a1" + csvName + "plain";
    txtPath = containerPath + txtName;
    
    System.out.println(toString());
  }

  @Override
  public String toString() {
    return "NameGenerator [conf=" + conf + ", container=" + container + ", containerPath=" + containerPath
        + ", parquetName=" + parquetName + ", parquetPath=" + parquetPath + ", jsonPath=" + jsonPath + ", jsonName="
        + jsonName + ", csvName=" + csvName + ", csvPath1=" + csvPath1 + ", csvPath2=" + csvPath2 + ", txtName="
        + txtName + ", txtPath=" + txtPath + ", identifier=" + identifier + ", dataRes=" + dataRes + ", dataResPath="
        + dataResPath + ", randomContainer=" + randomContainer + ", defaultFS="
        + defaultFS + ", getContainerPath()=" + getContainerPath() + ", getParquetPath()=" + getParquetPath()
        + ", getCsvPath1()=" + getCsvPath1() + ", getCsvPath2()=" + getCsvPath2() + ", getTxtPath()=" + getTxtPath()
        + ", getIdentifier()=" + getIdentifier() + ", getCsvName()=" + getCsvName() + ", getJsonPath()=" + getJsonPath()
        + ", getJsonName()=" + getJsonName() + ", getDataResPath()=" + getDataResPath() + ", getClass()=" + getClass()
        + ", hashCode()=" + hashCode() + ", toString()=" + super.toString() + "]";
  }

  public String getContainerPath() {
    return containerPath;
  }

  public void setContainerPath(String containerPath) {
    this.containerPath = containerPath;
  }

  public String getParquetPath() {
    return parquetPath;
  }

  public void setParquetPath(String parquetPath) {
    this.parquetPath = parquetPath;
  }

  public String getCsvPath1() {
    return csvPath1;
  }

  public void setCsvPath1(String csvPath1) {
    this.csvPath1 = csvPath1;
  }

  public String getCsvPath2() {
    return csvPath2;
  }

  public void setCsvPath2(String csvPath2) {
    this.csvPath2 = csvPath2;
  }

  public String getTxtPath() {
    return txtPath;
  }

  public void setTxtPath(String txtPath) {
    this.txtPath = txtPath;
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public String getCsvName() {
    return csvName;
  }

  public void setCsvName(String csvName) {
    this.csvName = csvName;
  }

  public String getJsonPath() {
    return jsonPath;
  }

  public void setJsonPath(String jsonPath) {
    this.jsonPath = jsonPath;
  }

  public String getJsonName() {
    return jsonName;
  }

  public void setJsonName(String jsonName) {
    this.jsonName = jsonName;
  }

  public String getDataResPath() {
    return dataResPath;
  }  

}
