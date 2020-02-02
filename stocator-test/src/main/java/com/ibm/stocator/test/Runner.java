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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class Runner {

  private static final int minBound = 10000;
  private static boolean dataCreate = true;
  private static boolean flatListing = false;
  private static String csvLocalLargePath = null;
  private static String csvLocalPath = null;
  private static boolean isTimeOutTest = true;
  private static boolean isSwift = false;
  
  public static void main(String[] args) throws Exception {
    String propertiesFile = null;
    if (args.length > 1) {
      csvLocalPath = args[0];
      propertiesFile = args[1];
    } else {
      System.out.println("Parameters required: path to csv file, path to properties file");
      System.exit(1);
    }
    if (args.length > 2) {
      dataCreate = Boolean.valueOf(args[2]).booleanValue();
    }
    if (args.length > 3) {
      flatListing = Boolean.valueOf(args[3]).booleanValue();
    }
    if (args.length > 4) {
      isTimeOutTest = Boolean.valueOf(args[4]).booleanValue();
    }      
    if (args.length > 5) {
      csvLocalLargePath = args[5];
    }
    ConfigHandler app = new ConfigHandler(propertiesFile);
    NameGenerator nameGenerator = new NameGenerator(app);
    
    SparkSession spark = createSparkSession(app, nameGenerator, null);
    long start = System.currentTimeMillis();
    executeTestSuite(nameGenerator, spark);
    long delta = System.currentTimeMillis() - start;
    System.out.println("Total run time: " + delta);
    
    String defaultFS = app.getProperty("object.storage.defaultFS");
    if (defaultFS != null) {
      spark = createSparkSession(app, nameGenerator, defaultFS);
      nameGenerator.setDefaultFS(true);
      start = System.currentTimeMillis();
      executeTestSuite(nameGenerator, spark);
      delta = System.currentTimeMillis() - start;
      System.out.println("Total run time: " + delta);      
    }

  }

private static SparkSession createSparkSession(ConfigHandler app,
    NameGenerator nameGenerator, String defaultFS) throws Exception{
  SparkSession spark = SparkSession
      .builder()
      .appName("Stocator test suite")
      .master("local")
      .getOrCreate();
  if (defaultFS != null) {
    spark.sparkContext().hadoopConfiguration().set("fs.defaultFS", defaultFS);
  }
  String objectStoreIdentifier = app.getProperty("object.storage.scheme");
  if (objectStoreIdentifier.equals("swift2d")) {
    isSwift = true;
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".impl",
        app.getProperty("object.storage.impl"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".auth.url",
        app.getProperty("auth.url"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".public",
        app.getProperty("public"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".tenant",
        app.getProperty("tenant"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".username",
        app.getProperty("username"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".auth.method",
        app.getProperty("auth.method"));
    spark.sparkContext().hadoopConfiguration().set(
        "fs." + nameGenerator.getIdentifier() + ".service." + app.getServiceName() + ".password",
        app.getProperty("password"));
    flatListing = true;
    nameGenerator.generateNewContainer(false);
  } else if (objectStoreIdentifier.equals("cos-iam")) {
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".impl",
        app.getProperty("object.storage.impl"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator." + nameGenerator.getIdentifier() + ".scheme",
        app.getProperty("object.storage.scheme"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator.scheme.list", app.getProperty("object.storage.scheme"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator." + nameGenerator.getIdentifier() + ".impl",
        app.getProperty("client.impl"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.endpoint",
        app.getProperty("endpoint"));
    if(app.getProperty("iam.api.key") != null) {
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.iam.api.key",
          app.getProperty("iam.api.key"));        
    } else {
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.iam.token",
          app.getProperty("iam.token"));
    }
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.iam.service.id",
        app.getProperty("iam.service.id"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.v2.signer.type",
        app.getProperty("v2.signer.type"));
    System.out.println("Setting default.client.exec.timeout = " + app.getProperty("default.client.exec.timeout"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.execution.timeout",
        app.getProperty("default.client.exec.timeout"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.request.timeout",
        app.getProperty("default.request.timeout"));
    if (!flatListing) {
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".flat.list",
          "false");        
    }
    nameGenerator.generateNewContainer(); 
  } else if (objectStoreIdentifier.equals("cos")) {
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".impl",
        app.getProperty("object.storage.impl"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator." + nameGenerator.getIdentifier() + ".scheme",
        app.getProperty("object.storage.scheme"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator.scheme.list", app.getProperty("object.storage.scheme"));
    spark.sparkContext().hadoopConfiguration().set("fs.stocator." + nameGenerator.getIdentifier() + ".impl",
        app.getProperty("client.impl"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.endpoint",
        app.getProperty("endpoint"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.access.key",
        app.getProperty("access.key"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.secret.key",
        app.getProperty("secret.key"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".service.v2.signer.type",
        app.getProperty("v2.signer.type"));
    System.out.println("Setting default.client.exec.timeout = " + app.getProperty("default.client.exec.timeout"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.execution.timeout",
        app.getProperty("default.client.exec.timeout"));
    spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.request.timeout",
        app.getProperty("default.request.timeout"));
    if (!flatListing) {
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".flat.list",
          "false");        
    }
    nameGenerator.generateNewContainer();
  } else if (objectStoreIdentifier.startsWith("s3a")) {
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".endpoint",
          app.getProperty("endpoint"));
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".access.key",
          app.getProperty("access.key"));
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".secret.key",
          app.getProperty("secret.key"));
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".connection.ssl.enabled",
          "false");
      System.out.println("Setting default.client.exec.timeout = " + app.getProperty("default.client.exec.timeout"));
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.execution.timeout",
          app.getProperty("default.client.exec.timeout"));
      spark.sparkContext().hadoopConfiguration().set("fs." + nameGenerator.getIdentifier() + ".client.request.timeout",
          app.getProperty("default.request.timeout"));
      nameGenerator.generateNewContainer();
    } else {
        throw new Exception("Unknown object store identifier");
  }
  return spark;
}

private static void executeTestSuite(NameGenerator nameGenerator,
    SparkSession spark) throws Exception{
  TestSuite testSuite = new TestSuite(dataCreate, flatListing);

  System.out.println("*********************************");
  System.out.println("*** Create dataframe from the local CSV file ***");
  Dataset<Row> schemaFlights = testSuite.getFlights(spark, csvLocalPath);
  
  nameGenerator.generateObjectNames();
  if (dataCreate) {
      System.out.println("Data cleanup (start) for " + nameGenerator.getContainerPath() + "*");
      System.out.println("*********************************");
      testSuite.deleteData(nameGenerator.getContainerPath(), spark.sparkContext().hadoopConfiguration(), false);
      System.out.println("*********************************");
  }

  testSuite.test1(spark, schemaFlights, nameGenerator.getCsvPath2());
  testSuite.test2(spark, schemaFlights, nameGenerator.getParquetPath(), Constants.PARQUET_TYPE);
  testSuite.test2(spark, schemaFlights, nameGenerator.getJsonPath(), Constants.JSON_TYPE);
  testSuite.test3(spark, schemaFlights, nameGenerator.getCsvPath1());
  testSuite.test4(spark, nameGenerator.getTxtPath());
  testSuite.test8(spark, nameGenerator.getTxtPath(), isTimeOutTest );

  if (isSwift) {
    nameGenerator.generateNewContainer("list");
    System.out.println("Data cleanup for " + nameGenerator.getContainerPath() + "*");
    System.out.println("*********************************");
    testSuite.deleteData(nameGenerator.getContainerPath(), spark.sparkContext().hadoopConfiguration(), dataCreate);
    System.out.println("*********************************");
  }
  testSuite.test6(spark, schemaFlights, nameGenerator.getContainerPath(), nameGenerator.getCsvName());
  if (isSwift) {
    nameGenerator.generateNewContainer(false);
    System.out.println("Data cleanup for " + nameGenerator.getContainerPath() + "*");
    System.out.println("*********************************");
    testSuite.deleteData(nameGenerator.getContainerPath(), spark.sparkContext().hadoopConfiguration(), dataCreate);
    System.out.println("*********************************");
  }
  
  testSuite.test7(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.TEXT_TYPE);
  testSuite.test7(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.JSON_TYPE);
  testSuite.test7(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test71(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.TEXT_TYPE);
  testSuite.test71(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.JSON_TYPE);
  testSuite.test71(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test10(spark, nameGenerator.getDataResPath() + "/dfp");
  testSuite.test11(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test12(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test9(spark, nameGenerator.getDataResPath());
  testSuite.test13(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.CSV_TYPE);
  testSuite.test14(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.JSON_TYPE);
  testSuite.test14(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test15(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.JSON_TYPE);
  testSuite.test15(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  testSuite.test16(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.JSON_TYPE);
  testSuite.test16(spark, schemaFlights, nameGenerator.getContainerPath(), Constants.PARQUET_TYPE);
  
  if (csvLocalLargePath != null) {
    System.out.println("*********************************");
    System.out.println("Large file test!");
    Dataset<Row> largeSchemaFlights = testSuite.getFlights(spark, csvLocalLargePath);
    if (isSwift) {
      nameGenerator.generateNewContainer(true);
    }
    testSuite.test1(spark, largeSchemaFlights, nameGenerator.getCsvPath2());
    testSuite.test2(spark, largeSchemaFlights, nameGenerator.getParquetPath(), Constants.PARQUET_TYPE);
    testSuite.test2(spark, largeSchemaFlights, nameGenerator.getJsonPath(), Constants.JSON_TYPE);
    System.out.println("***** Repartition to 1");
    largeSchemaFlights.repartition(1);
    if (isSwift) {
      nameGenerator.generateNewContainer(true);
    }
    testSuite.test2(spark, largeSchemaFlights, nameGenerator.getParquetPath(), Constants.PARQUET_TYPE);
    testSuite.test2(spark, largeSchemaFlights, nameGenerator.getJsonPath(), Constants.JSON_TYPE);
  } else {
    System.out.println("*********************************");
    System.out.println("No large file test to be executed");
  }

}
}
