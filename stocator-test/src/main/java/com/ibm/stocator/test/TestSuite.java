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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.guava.collect.ImmutableList;

import com.ibm.stocator.test.datatype.Cube;
import com.ibm.stocator.test.datatype.Square;

public class TestSuite implements Serializable {
  boolean dataCreate;
  boolean flatListing;

  public TestSuite() {
    dataCreate = false;
  }

  public TestSuite(boolean dataCreateT, boolean flatListingT) {
    dataCreate = dataCreateT;
    flatListing = flatListingT;
  }

  /**
   * Delete given path
   *
   * @param dataPath data path
   * @param conf Hadoop configuration
   * @param forced force delete
   * @throws Exception if error
   */
  public void deleteData(String dataPath, Configuration conf, boolean forced) throws Exception {
    System.out.println("**** Delete " + dataPath);
    if (!forced) {
      return;
    }
    FileSystem fs = null;
    String defaultFS = conf.get("fs.defaultFS");
    if (defaultFS != null && !defaultFS.equals("file:///")) {
      System.out.println("delete fs.defaultFS = " + conf.get("fs.defaultFS"));
      fs = FileSystem.get(conf);
    } else {
      fs = FileSystem.get(URI.create(dataPath), conf);
    }
    try {
      if (dataPath.isEmpty()) {
        fs.delete(new Path("*"), true);
      } else {
        fs.delete(new Path(dataPath), true);
      }
    } catch (FileNotFoundException e) {
      System.out.println("Nothing to delete " + dataPath + ", continue");
    } catch (IOException e) {
      System.out.println("Failed to delete " + dataPath);
      throw e;
    }
  }

  public FileSystem getFs(String dataPath, Configuration conf) throws Exception {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(dataPath), conf);
      return fs;
    } catch (IOException e) {
      System.out.println("Failed to get FS " + dataPath);
      throw e;
    }

  }

  private void countAndCompare(Dataset<Row> inSpark, long readRecords, String msg) throws Exception {
    long totalInSpark = inSpark.count();
    if (totalInSpark != readRecords) {
      System.out.println("*********************************");
      System.out.println(msg + ": Records that were written into object store doesn't match");
      System.out.println(msg + ": Readed from object store: " + readRecords + ", expected: " + totalInSpark);
      throw new Exception(msg + ": Readed from object store: " + readRecords + ", expected: " + totalInSpark);
    } else {
      System.out.println(
          msg + " Completed successfully. Readed from object store: " + readRecords + ", expected: " + totalInSpark);
    }
  }

  private void countAndCompare(long totalInSpark, long readRecords, String msg, long baseCount) throws Exception {
    if (totalInSpark != readRecords) {
      System.out.println("*********************************");
      System.out.println(msg + ": Records that were written into object store doesn't match");
      System.out.println("Flast listing set to " + String.valueOf(flatListing));
      System.out.println(msg + ": Readed from object store: " + readRecords + ", expected: " + totalInSpark
          + ", base count: " + baseCount);
      throw new Exception(msg + ": Readed from object store: " + readRecords + ", expected: " + totalInSpark
          + ", base count: " + baseCount);
    } else {
      System.out.println("*********************************");
      System.out.println(msg + " Completed successfully. Readed from object store: " + readRecords + ", expected: "
          + totalInSpark + ", base count: " + baseCount);
      System.out.println("*********************************");
    }
  }

  public void test1(SparkSession spark, Dataset<Row> schemaFlights, String outCSV2) throws Exception {
    try {
      System.out.println("*********************************");
      System.out.println("T1: Save flights data as CSV object " + outCSV2);
      schemaFlights.write().mode("overwrite").format("com.databricks.spark.csv").save(outCSV2);
      // read and compare
      System.out.println("*********************************");
      System.out.println("T1: Read query CSV object and compare.");
      JavaRDD<String> csvReadPlainBack = spark.read().textFile(outCSV2).javaRDD();
      System.out.println("T1: Read back :" + csvReadPlainBack.count());
      countAndCompare(schemaFlights, csvReadPlainBack.count(), "T1");
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(outCSV2, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }
  }

  public void test2(SparkSession spark, Dataset<Row> schemaFlights, String out, String type) throws Exception {
    try {
      System.out.println("*********************************");
      System.out.println("T2: Save flights data as " + type + " object: " + out);
      // schemaFlights.write().mode("overwrite").save(out);
      if (type.equals(Constants.PARQUET_TYPE)) {
        schemaFlights.write().mode("overwrite").parquet(out);
      } else {
        schemaFlights.write().mode("overwrite").json(out);
      }
      // read object back
      System.out.println("T2: Read flights data as " + type + " object: " + out);
      Dataset<Row> pFlights;
      if (type.equals(Constants.PARQUET_TYPE)) {
        pFlights = spark.read().parquet(out);
      } else {
        pFlights = spark.read().json(out);
      }
      // Let's count records
      System.out.println("T2: Compare records that was read to records that were generated by Spark");
      countAndCompare(schemaFlights, pFlights.count(), "T2");
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(out, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }

  }

  public void test3(SparkSession spark, Dataset<Row> schemaFlights, String outCSV1) throws Exception {
    try {
      System.out.println("*********************************");
      System.out.println("T3: Run SQL query on the dataset");
      schemaFlights.createOrReplaceTempView("flights");
      Dataset<Row> results = spark.sql(
          "select year, month, count(*) as Count  from flights WHERE cancellationCode like 'B' GROUP BY year, month ORDER By year, month");
      System.out.println("*********************************");
      System.out.println("T3: Save query result as CSV object: ");
      System.out.println("T3 " + outCSV1);
      results.write().mode("overwrite").format("com.databricks.spark.csv").save(outCSV1);
      // read and compare
      System.out.println("*********************************");
      System.out.println("T3: Read query CSV object and compare");
      JavaRDD<String> csvReadBack = spark.read().textFile(outCSV1).javaRDD();
      countAndCompare(results, csvReadBack.count(), "T3");
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(outCSV1, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }

  }

  public void test6(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String csvOutName)
      throws Exception {
    System.out.println("*********************************");
    // store 3 csv objects in the container
    System.out.println("T6: Going to store 3 CSV objects in the new container");
    try {
      for (int i = 1; i < 2; i++) {
        String csvOut = containerOut + csvOutName + "plain" + i;
        System.out.println("T6: Store: " + csvOut);
        schemaFlights.write().mode("overwrite").format("com.databricks.spark.csv").save(csvOut);
      }
      // count data of 3 objects
      System.out.println("Going to sleep for 20 sec");
      System.out.println("*********************************");
      System.out.println("T6: Going to read CSV objects from container: " + containerOut);
      JavaRDD<String> csvRes = spark.read().textFile(containerOut + "*").javaRDD();
      System.out.println("T6: Going to compare records " + containerOut);
      long baseCount = schemaFlights.count();
      countAndCompare(baseCount * 1, csvRes.count(), "T6", baseCount);
    } catch (Exception e) {
      throw e;
    } finally {
      // cleanup
      for (int i = 1; i < 2; i++) {
        String csvOut = containerOut + csvOutName + "plain" + i;
        System.out.println("T6: (Finally) delete: " + csvOut);
        deleteData(csvOut, spark.sparkContext().hadoopConfiguration(), dataCreate);
      }
    }
  }

  public void test4(SparkSession spark, String outText1) throws Exception {
    try {
      System.out.println("*********************************");
      System.out.println("T4: Create collection and store it as text file in " + outText1);
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      JavaRDD<Integer> distData = new JavaSparkContext(spark.sparkContext()).parallelize(data);
      distData.saveAsTextFile(outText1);
      JavaRDD<String> txtRes = spark.read().textFile(outText1).javaRDD();
      long baseCount = txtRes.count();
      countAndCompare(baseCount, distData.count(), "T4", baseCount);
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(outText1, spark.sparkContext().hadoopConfiguration(), true);
    }

  }

  /**
   * container/a/b/c/data.csv container/a/b/c/d/data.csv
   * container/a/b/c/d/data1.csv container/a/b/c/d/newdata.csv
   * container/json/store container/json/store_returns
   * container/json/store_sales container/test.csv container/123test.csv
   * 
   * @param sc
   * @param schemaFlights
   * @param containerOut
   * @param csvOutName
   * @throws Exception
   */
  public void test7(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type) throws Exception {
    System.out.println("*********************************");
    System.out.println("T7: Going to create nested structures and check globber on " + containerOut);
    String o1 = containerOut + "a/b/c/data." + type;
    String o2 = containerOut + "a/b/c/d/data." + type;
    String o3 = containerOut + "a/b/c/d/data1." + type;
    String o4 = containerOut + "a/b/c/d/newdata." + type;
    String o5 = containerOut + "test." + type;
    String o6 = containerOut + "123test." + type;
    String o7 = containerOut + "json/store";
    String o8 = containerOut + "json/store_sales";
    String o9 = containerOut + "json/store_returns";
    String o10 = containerOut + "store";
    String o11 = containerOut + "store_sales";
    String o12 = containerOut + "store_returns";
    String o13 = containerOut + "/mtb/data1." + type;
    String o14 = containerOut + "/mtb/data2." + type;
    
    String[] data = {o1, o2, o3, o4, o5, o6, o7,o8, o9, o10, o11, o12, o13, o14};

    try {
      if (dataCreate) {
        for (String entry: data) {
          createObject("T7", schemaFlights, entry, type);
        }
      }
      long baseCount = schemaFlights.count();
      // test 1
      // container/* should bring all the objects
      String path = containerOut + "*";

      System.out.println(
          "***T7-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-1-" + type, type, path, spark, baseCount, 14);
      } else {
        readAndTest("T7-1-" + type, type, path, spark, baseCount, 5);
      }

      // test 1-1
      path = containerOut + "mtb/*. + type";
      System.out.println(
          "***T7-1-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-1-1-" + type, type, path, spark, baseCount, 2);
      } else {
        readAndTest("T7-1-1-" + type, type, path, spark, baseCount, 2);
      }

      // test 1
      // container/* should bring all the objects
      path = containerOut + "*. + type";
      System.out.println(
          "***T7-1-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-1-1-" + type, type, path, spark, baseCount, 8);
      } else {
        readAndTest("T7-1-1-" + type, type, path, spark, baseCount, 0);
      }

      // test 2
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/c/d/*";
      System.out.println(
          "***T7-2 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-2-" + type, type, path, spark, baseCount, 3);

      // test 2
      // container/a/b/c/d* should bring 1 objects
      path = containerOut + "a/b/c/d*";
      System.out.println(
          "***T7-2-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-2-1-" + type, type, path, spark, baseCount, 4);
      } else {
        readAndTest("T7-2-1-" + type, type, path, spark, baseCount, 1);
      }

      // test 3
      // container/a/b/c/d/data.csv should bring 1 objects
      path = containerOut + "a/b/c/d/data." + type;
      System.out.println(
          "***T7-3 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-3-" + type, type, path, spark, baseCount, 1);
      // test 4
      // container/a/b/c/d/data* should bring 2 objects
      path = containerOut + "a/b/c/d/data*";
      System.out.println(
          "***T7-4 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-4-" + type, type, path, spark, baseCount, 2);
      // test 5
      // container/te* should bring 1 objects
      path = containerOut + "te*";
      System.out.println(
          "***T7-5 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-5-" + type, type, path, spark, baseCount, 1);

      // test 6
      // container/123test.csv should bring 1 objects
      path = o6;
      System.out.println(
          "***T7-6 : Reading " + path + "  from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-6-" + type, type, path, spark, baseCount, 1);

      // test 7
      // container/json/store should bring 1 objects
      path = containerOut + "json/store";
      System.out.println(
          "***T7-7 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-7-" + type, type, path, spark, baseCount, 1);

      // test 8
      // container/json/store should bring 1 objects
      path = containerOut + "json/store*";
      System.out.println(
          "***T7-8 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-8-" + type, type, path, spark, baseCount, 3);

      // test 9
      // container/json/store should bring 1 objects
      path = containerOut + "store";
      System.out.println(
          "***T7-9 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-9-" + type, type, path, spark, baseCount, 1);

      // test 10
      // container/json/store should bring 1 objects
      path = containerOut + "store*";
      System.out.println(
          "***T7-10 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-10-" + type, type, path, spark, baseCount, 3);

      // test 11
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/*";
      System.out.println(
          "***T7-11 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-11-" + type, type, path, spark, baseCount, 4);
      } else {
        readAndTest("T7-11-" + type, type, path, spark, baseCount, 0);
      }

      // test 12
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b";
      System.out.println(
          "***T7-12 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-12-" + type, type, path, spark, baseCount, 0);

      // test 13
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/c/*";
      System.out.println(
          "***T7-13 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-13-" + type, type, path, spark, baseCount, 4);
      } else {
        readAndTest("T7-13-" + type, type, path, spark, baseCount, 1);
      }

      // test 14
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/c";
      System.out.println(
          "***T7-14 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-14-" + type, type, path, spark, baseCount, 0);

      // test 15
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/c*";
      System.out.println(
          "***T7-15 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T7-15-" + type, type, path, spark, baseCount, 4);
      } else {
        readAndTest("T7-15-" + type, type, path, spark, baseCount, 0);
      }

      // test 16
      // container/a/b/c/d/* should bring 3 objects
      path = containerOut + "a/b/c/";
      System.out.println(
          "***T7-16 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T7-16-" + type, type, path, spark, baseCount, 0);

      System.out.println("Finally  - delete temp data");
      if (dataCreate) {
        for (String entry: data) {
          deleteData(entry, spark.sparkContext().hadoopConfiguration(), dataCreate);
        }
      }
      System.out.println("Data was re-created and deleted");

    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * container/a/b/c/data.csv container/a/b/c/d/data.csv
   * container/a/b/c/d/data1.csv container/a/b/c/d/newdata.csv
   * container/json/store container/json/store_returns
   * container/json/store_sales container/test.csv container/123test.csv
   * 
   * @param sc
   * @param schemaFlights
   * @param containerOut
   * @param csvOutName
   * @throws Exception
   */
  public void test71(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type) throws Exception {
    System.out.println("*********************************");
    System.out.println("T71: Going to create nested structures and check globber on " + containerOut);
    String o1 = containerOut + "year=2016/month=1/data." + type;
    String o10 = containerOut + "year=2016/month=10/data." + type;
    String o11 = containerOut + "year=2016/month=11/data." + type;
    String o12 = containerOut + "year=2016/month=12/data." + type;
    String o2 = containerOut + "year=2016/month=2/data." + type;
    String o3 = containerOut + "year=2016/month=3/data." + type;
    String o4 = containerOut + "year=2016/month=4/data." + type;
    String o5 = containerOut + "year=2016/month=5/data." + type;
    String o6 = containerOut + "year=2016/month=6/data." + type;
    String o7 = containerOut + "year=2016/month=7/data." + type;
    String o8 = containerOut + "year=2016/month=8/data." + type;
    String o9 = containerOut + "year=2016/month=9/data." + type;
    String oo = containerOut + "year=2016/month=10";
    
    String[] data = {o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, oo};

    try {
      if (dataCreate) {
        for (String entry: data) {
          createObject("T71", schemaFlights, entry, type);
        }
      }
      long baseCount = schemaFlights.count();
      // test 1
      // container/* should bring all the objects
      String path = containerOut + "year=2016/";

      System.out.println(
          "***T71-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T71-1-" + type, type, path, spark, baseCount, 12);
      } else {
        readAndTest("T71-1-" + type, type, path, spark, baseCount, 12);
      }

      System.out.println("Finally  - delete temp data");
      if (dataCreate) {
        for (String entry: data) {
          deleteData(entry, spark.sparkContext().hadoopConfiguration(), dataCreate);
        }
      }
      System.out.println("Data was re-created and deleted");

    } catch (Exception e) {
      throw e;
    }
  }

  private void createObject(String msg, Dataset<Row> df, String name, String type) {
    System.out.println(msg + ": " + name + " type: " + type);
    if (type.equals(Constants.CSV_TYPE)) {
      df.write().mode("overwrite").format("com.databricks.spark.csv").save(name);
    } else if (type.equals(Constants.PARQUET_TYPE)) {
      df.write().mode("overwrite").parquet(name);
    } else if (type.equals(Constants.JSON_TYPE)) {
      df.write().mode("overwrite").json(name);
    }
  }

  private void createAppendObject(String msg, Dataset<Row> df, String name, String type) {
    System.out.println(msg + ": " + name + " type: " + type);
    if (type.equals(Constants.CSV_TYPE)) {
      df.write().mode("append").format("com.databricks.spark.csv").save(name);
    } else if (type.equals(Constants.PARQUET_TYPE)) {
      df.write().mode("append").parquet(name);
    } else if (type.equals(Constants.JSON_TYPE)) {
      df.write().mode("append").json(name);
    }
  }

  private void createNonOverwriteObject(String msg, Dataset<Row> df, String name, String type) {
    System.out.println(msg + ": " + name + " type: " + type);
    if (type.equals(Constants.CSV_TYPE)) {
      df.write().format("com.databricks.spark.csv").save(name);
    } else if (type.equals(Constants.PARQUET_TYPE)) {
      df.write().parquet(name);
    } else if (type.equals(Constants.JSON_TYPE)) {
      df.write().json(name);
    }
  }

  private void readAndTest(String msg, String type, String path, SparkSession spark, long baseCount, int factor)
      throws Exception {
    if (type.equals(Constants.TEXT_TYPE)) {
      try {
        JavaRDD<String> res = spark.read().textFile(path).javaRDD();
        countAndCompare(baseCount * factor, res.count(), msg, baseCount);
      } catch (org.apache.spark.sql.AnalysisException e) {
        System.out.println("schema not present, since object not found");
      }
    } else if (type.equals(Constants.PARQUET_TYPE)) {
      try {
        Dataset<Row> pFlights = spark.read().parquet(path);
        countAndCompare(baseCount * factor, pFlights.count(), msg, baseCount);
      } catch (org.apache.spark.sql.AnalysisException e) {
        System.out.println("schema not present, since object not found");
      }
    } else if (type.equals(Constants.CSV_TYPE)) {
      try {
        JavaRDD<Row> pFlights = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
            .option("header", "true").option("inferSchema", "true").load(path).javaRDD();
        countAndCompare(baseCount * factor, pFlights.count(), msg, baseCount);
      } catch (org.apache.spark.sql.AnalysisException e) {
        System.out.println("schema not present, since object not found");
      }
    } else {
      try {
        Dataset<Row> pFlights = spark.read().json(path);
        countAndCompare(baseCount * factor, pFlights.count(), msg, baseCount);
      } catch (org.apache.spark.sql.AnalysisException e) {
        System.out.println("schema not present, since object not found");
      }
    }

  }

  public void test8(SparkSession spark, String outText1, boolean isTimeOutTest) throws Exception {
    try {
      System.out.println("*********************************");
      System.out.println("T8: Timeout retry test. Please wait with patience");
      System.out.println("T8:Create collection and store it as text file in " + outText1);
      List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
      JavaRDD<Integer> distData = new JavaSparkContext(spark.sparkContext()).parallelize(data);
      distData.saveAsTextFile(outText1);
      JavaRDD<String> txtRes = spark.read().textFile(outText1).javaRDD();
      long baseCount = txtRes.count();
      countAndCompare(baseCount, distData.count(), "T8", baseCount);
      if (isTimeOutTest) {
        System.out.println("T8: Sleep for 10 minutes ");
        Thread.sleep(10 * 60 * 1000);
        System.out.println("T8: About to wake up");
        System.out.println("T8: Re-define data source");
      }
      txtRes = spark.read().textFile(outText1).javaRDD();
      baseCount = txtRes.count();
      countAndCompare(baseCount, distData.count(), "T8", baseCount);
      System.out.println("T8: Sleep for 10 minutes ");

    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(outText1, spark.sparkContext().hadoopConfiguration(), true);
    }

  }

  public void test9(SparkSession spark, String path) throws Exception {
    System.out.println("*********************************");
    System.out.println("T9: Create two schemas with partition. Merge schemas");
    try {
      List<Square> squares = new ArrayList<Square>();
      for (int value = 1; value <= 5; value++) {
        Square square = new Square();
        square.setValue(value);
        square.setSquare(value * value);
        squares.add(square);
      }
      Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
      squaresDF.write().parquet(path + "/test_table/key=1");

      List<Cube> cubes = new ArrayList<Cube>();
      for (int value = 6; value <= 10; value++) {
        Cube cube = new Cube();
        cube.setValue(value);
        cube.setCube(value * value * value);
        cubes.add(cube);
      }
      Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
      cubesDF.write().parquet(path + "/test_table/key=2");

      Dataset<Row> mergedDF = spark.read().option("mergeSchema", "true").parquet(path + "/test_table");
      mergedDF.printSchema();
      if (mergedDF.schema().length() != 4) {
        throw new Exception("T9 failed. Merged schema has " + mergedDF.schema().length() + " keys, while expected 4");
      }
      System.out.println(mergedDF.schema().length());
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(path, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }

  }

  public void test10(SparkSession spark, String path) throws Exception {
    System.out.println("*********************************");
    System.out.println("T10: Partition test - start");
    try {
      List<Square> squares = new ArrayList<Square>();
      for (int value = 1; value <= 10; value++) {
        Square square = new Square();
        square.setValue(value);
        square.setSquare(value * value);
        squares.add(square);
      }
      Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
      squaresDF.write().mode("overwrite").partitionBy("value").parquet(path);

      Dataset<Row> readDF = spark.read().parquet(path);
      readDF.printSchema();
      long dfCount = readDF.count();
      long schemaLength = readDF.schema().length();
      if (dfCount != 10) {
        throw new Exception("T10: failed. Read " + dfCount + ", expected 10");
      }
      if (schemaLength != 2) {
        throw new Exception("T10: failed. Schema length" + schemaLength + ", expected 2");
      }
      System.out.println("T10: Partition test - completed");
    } catch (Exception e) {
      throw e;
    } 
      finally { deleteData(path, spark.sparkContext().hadoopConfiguration(), dataCreate);
      }

  }

  public void test11(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T11: Going to create nested structures and check globber on " + containerOut);
    String o1 = containerOut + "Dir/SubDir/File1." + type;
    String o2 = containerOut + "Dir/SubDir/File2" + type;
    String o3 = containerOut + "Dir/File1" + type;
    
    String[] data = {o1, o2, o3};
    try {
      if (dataCreate) {
        for (String entry: data) {
          createObject("T11", schemaFlights, entry, type);
        }
      }
      long baseCount = schemaFlights.count();
      // test 1
      // container/* should bring 5 objects
      String path = containerOut + "Dir*";

      System.out.println(
          "***T11-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      if (flatListing) {
        readAndTest("T11-1-" + type, type, path, spark, baseCount, 3);
      } else {
        readAndTest("T11-1-" + type, type, path, spark, baseCount, 0);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      for (String entry: data) {
        deleteData(entry, spark.sparkContext().hadoopConfiguration(), dataCreate);
      }
    }
  }

  public void test12(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T12: Going to create nested structures and check globber on " + containerOut);
    String o1 = containerOut + "Dir/SubDir1/abc/File1." + type;
    String o2 = containerOut + "Dir/SubDir1/abc/File2." + type;
    String o3 = containerOut + "Dir/SubDir1/abc/File3.abc";
    String o4 = containerOut + "Dir/File2." + type;
    String o5 = containerOut + "Dira/File2." + type;
    String o6 = containerOut + "Dir/SubDir2/abc/File2." + type;
    
    String[] data = {o1, o2, o3, o4, o5, o6};

    try {
      if (dataCreate) {
        for (String entry: data) {
          createObject("T12", schemaFlights, entry, type);
        }
      }
      long baseCount = schemaFlights.count();

      String path1 = containerOut + "Dir*";
      String path2 = containerOut + "Dir/*";
      String path3 = containerOut + "Dir/SubDir1/abc/File1*";
      String path4 = containerOut + "Dir/SubDir1/abc/*.txt";
      String path5 = containerOut + "Dir/*.abc";
      String path6 = containerOut + "Dir/*/abc/*." + type;

      if (flatListing) {
        System.out.println(
            "***T12-1 : Reading " + path1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-1-" + type, type, path1, spark, baseCount, 6);
        System.out.println(
            "***T12-2 : Reading " + path2 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-2-" + type, type, path2, spark, baseCount, 5);
        System.out.println(
            "***T12-3 : Reading " + path3 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-3-" + type, type, path3, spark, baseCount, 1);
        System.out.println(
            "***T12-4 : Reading " + path4 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-4-" + type, type, path4, spark, baseCount, 0);
        System.out.println(
            "***T12-5 : Reading " + path5 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-5-" + type, type, path5, spark, baseCount, 1);
        System.out.println(
            "***T12-6 : Reading " + path6 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-6-" + type, type, path6, spark, baseCount, 3);
      } else {
        System.out.println(
            "***T12-1 : Reading " + path1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-1-" + type, type, path1, spark, baseCount, 6);
        System.out.println(
            "***T12-2 : Reading " + path2 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-2-" + type, type, path2, spark, baseCount, 1);
        System.out.println(
            "***T12-3 : Reading " + path3 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-3-" + type, type, path3, spark, baseCount, 1);
        System.out.println(
            "***T12-4 : Reading " + path4 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-4-" + type, type, path4, spark, baseCount, 0);
        System.out.println(
            "***T12-5 : Reading " + path5 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-5-" + type, type, path5, spark, baseCount, 1);
        System.out.println(
            "***T12-6 : Reading " + path6 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
        readAndTest("T12-6-" + type, type, path6, spark, baseCount, 3);
      }
    } catch (Exception e) {
      throw e;
    } finally {
      for (String entry: data) {
        deleteData(entry, spark.sparkContext().hadoopConfiguration(), dataCreate);
      }
    }
  }

  public void test13(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T13: Going to create nested structures and check globber on " + containerOut);
    String o1 = containerOut + "Dir/result/jobid=21274501-57a1-4690-9a84-9d2294fcf64d";
    try {
      if (dataCreate) {
        createObject("T13", schemaFlights, o1, type);
      }
      long baseCount = schemaFlights.count();
      String path = containerOut + "Dir/result/jobid=21274501-57a1-4690-9a84-9d2294fcf64";

      System.out.println(
          "***T13-1 : Reading " + path + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T13-1-" + type, type, path, spark, baseCount, 1);
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(o1, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }
  }

  public void test14(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T14: Append mode " + containerOut);
    String o1 = containerOut + "myData";
    try {
      createAppendObject("T14 - first append", schemaFlights, o1, type);
      long baseCount = schemaFlights.count();
      System.out
          .println("***T14-1 : Reading " + o1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T14-1-" + type, type, o1, spark, baseCount, 1);
      createAppendObject("T14 - second append", schemaFlights, o1, type);
      baseCount = schemaFlights.count();
      System.out
          .println("***T14-2 : Reading " + o1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T14-2-" + type, type, o1, spark, baseCount, 2);
    } catch (Exception e) {
      throw e;
    } finally {
      deleteData(o1, spark.sparkContext().hadoopConfiguration(), true);
    }
  }

  public void test15(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T15: Non overwrite mode " + containerOut);
    String o1 = containerOut + "myData";
    try {
      createNonOverwriteObject("T15 - non overwrite ", schemaFlights, o1, type);
      long baseCount = schemaFlights.count();
      System.out
          .println("***T15-1 : Reading " + o1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T15-1-" + type, type, o1, spark, baseCount, 1);
    } catch (Exception e) {
      throw e;
    }
    try {
      createNonOverwriteObject("T15 - second overwrite", schemaFlights, o1, type);
      long baseCount = schemaFlights.count();
      System.out
          .println("***T15-2 : Reading " + o1 + " from " + containerOut + ", base unit " + baseCount + " type " + type);
      readAndTest("T15-2-" + type, type, o1, spark, baseCount, 1);
    } catch (Exception e) {
      if (e.getMessage().contains("already exists.")) {
        System.out.println("Expected error: " + e.getMessage());
      } else {
        throw e;
      }
    } finally {
      deleteData(o1, spark.sparkContext().hadoopConfiguration(), true);
    }

  }

  public void test16(SparkSession spark, Dataset<Row> schemaFlights, String containerOut, String type)
      throws Exception {
    System.out.println("*********************************");
    System.out.println("T16: Non overwrite mode " + containerOut);
    String o1 = containerOut + "myData/123";
    StructType schema = DataTypes
        .createStructType(new StructField[] { DataTypes.createStructField("NAME", DataTypes.StringType, false),
            DataTypes.createStructField("STRING_VALUE", DataTypes.StringType, false),
            DataTypes.createStructField("NUM_VALUE", DataTypes.IntegerType, false), });
    Row r1 = RowFactory.create("name1", "value1", 1);
    Row r2 = RowFactory.create("name2", "value2", 2);
    List<Row> rowList = ImmutableList.of(r1, r2);
    Dataset<Row> rows = spark.createDataFrame(rowList, schema);
    try {
      if (type.equals(Constants.PARQUET_TYPE)) {
        rows.write().mode(SaveMode.Overwrite).parquet(o1);
      } else if (type.equals(Constants.JSON_TYPE)) {
        rows.write().mode(SaveMode.Overwrite).json(o1);
      }
    } catch (Exception e) {
      deleteData(o1, spark.sparkContext().hadoopConfiguration(), dataCreate);
      throw e;
    } finally {
      deleteData(o1, spark.sparkContext().hadoopConfiguration(), dataCreate);
    }
  }

  public Dataset<Row> getFlights(SparkSession spark, String csvLocalPath) {
    JavaRDD<FlightsSchema> flights = spark.read().textFile(csvLocalPath).javaRDD()
        .map(new Function<String, FlightsSchema>() {
          public FlightsSchema call(String line) throws Exception {
            String[] parts = line.split(",");

            FlightsSchema flight = new FlightsSchema();
            flight.setYear(Integer.parseInt(parts[0].trim()));
            flight.setMonth(Integer.parseInt(parts[1].trim()));
            flight.setDayofMonth(Integer.parseInt(parts[2].trim()));
            flight.setDayOfWeek(Integer.parseInt(parts[3].trim()));
            flight.setDepTime(parts[4]);
            flight.setcRSDepTime(parts[5]);
            flight.setArrTime(parts[6]);
            flight.setcRSArrTime(parts[7]);
            flight.setUniqueCarrier(parts[8]);
            flight.setFlightNum(parts[9]);
            flight.setTailNum(parts[10]);
            flight.setActualElapsedTime(parts[11]);
            flight.setcRSElapsedTime(parts[12]);
            flight.setAirTime(parts[13]);
            flight.setArrDelay(parts[14]);
            flight.setDepDelay(parts[15]);
            flight.setOrigin(parts[16]);
            flight.setDest(parts[17]);
            flight.setDistance(parts[18]);
            flight.setTaxiIn(parts[19]);
            flight.setTaxiOut(parts[20]);
            flight.setCancelled(parts[21]);
            flight.setCancellationCode(parts[22]);
            flight.setDiverted(parts[23]);
            flight.setCarrierDelay(parts[24]);
            flight.setWeatherDelay(parts[25]);
            flight.setnASDelay(parts[26]);
            flight.setSecurityDelay(parts[27]);
            flight.setLateAircraftDelay(parts[28]);

            return flight;
          }
        });
    Dataset<Row> schemaFlights = spark.createDataFrame(flights, FlightsSchema.class);
    return schemaFlights;
  }

}
