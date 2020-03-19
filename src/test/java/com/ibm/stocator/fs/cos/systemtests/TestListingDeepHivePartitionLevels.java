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
 */

package com.ibm.stocator.fs.cos.systemtests;

import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import com.ibm.stocator.fs.common.StocatorPath;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;
import static com.ibm.stocator.fs.common.FileSystemTestUtils.dumpStats;

/**
 * Test listing time deep hive style partitioning with and without flat listing turned on
 */
public class TestListingDeepHivePartitionLevels extends COSFileSystemBaseTest {

  private static Path[] sTestData;
  private static Path[] sEmptyFiles;
  private static Path[] sEmptyDirs;
  private static byte[] sData = "This is file".getBytes();
  private String pattern1 = "_temporary/st_ID/_temporary/attempt_ID/";
  private StocatorPath stocPath;
  private static Hashtable<String, String> sConfFlatListing = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConfFlatListing.put("fs.cos.flat.list", "false");
    createCOSFileSystem(sConfFlatListing);
    if (sFileSystem != null) {
      createTestData();
    }
  }

  @Before
  public final void before() {
    Configuration conf = new Configuration();
    conf.setStrings("fs.stocator.temp.identifier", pattern1);
    stocPath = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, sBaseURI);
  }

  private static void createTestData() throws IOException {
    sTestData = new Path[] {
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/Year=2019/"
            + "Month=5/Day=9/Hour=12/part-00000-04d683e3-c189-479b-b661-b01c152357d3"
            + "-attempt_20200308122917_0000_m_000000_0.c000.snappy.parquet"),
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/Year=2019/"
            + "Month=5/Day=9/Hour=12/part-00001-04d683e3-c189-479b-b661-b01c152357d3"
            + "-attempt_20200308122917_0000_m_000001_0.c000.snappy.parquet"),
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-10-12/Year=2019/"
           + "Month=5/Day=10/Hour=12/part-00000-4fa834da-1a6d-4bca-a3c9-df9233616217"
           + "-attempt_20200308122921_0001_m_000000_0.c000.snappy.parquet"),
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-10-12/Year=2019/"
            + "Month=5/Day=10/Hour=12/part-00001-4fa834da-1a6d-4bca-a3c9-df9233616217"
            + "-attempt_20200308122921_0001_m_000001_0.c000.snappy.parquet")};

    sEmptyFiles = new Path[] {
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/_SUCCESS"),
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-10-12/_SUCCESS")};

    sEmptyDirs = new Path[] {
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/_temporary/0/"),
        new Path(sBaseURI + "/tmp/manyHivePartitions/log_write_time=2019-05-10-12/_temporary/0/")};

    for (Path path : sTestData) {
      createFile(path, sData);
    }
    for (Path path : sEmptyFiles) {
      createEmptyFile(path);
    }
    for (Path path : sEmptyDirs) {
      createDirectory(path);
    }

  }

  @Test
  public void testListGlobber() throws Exception {
    Path dataPath = new Path(sBaseURI + "/tmp/manyHivePartitions");

    // Check with flat listing turned off
    // Check now with flat listing turned on
    sConfFlatListing.put("fs.cos.flat.list", "true");
    createCOSFileSystem(sConfFlatListing);
    long startTime = System.nanoTime();
    FileStatus[] listingRes = sFileSystem.listStatus(dataPath);
    long endTime = System.nanoTime();
    long listingTimeFlatListing = endTime - startTime;
    System.out.println("Stocator listing took for dataset flat=true "
            + listingTimeFlatListing + "ns");

    // Check with flat listing turned off
    // Check now with flat listing turned on
    sConfFlatListing.put("fs.cos.flat.list", "false");
    createCOSFileSystem(sConfFlatListing);
    startTime = System.nanoTime();
    listingRes = sFileSystem.listStatus(dataPath);
    endTime = System.nanoTime();
    listingTimeFlatListing = endTime - startTime;
    System.out.println("Stocator listing took for dataset flat=false "
            + listingTimeFlatListing + "ns");

  }

  @Test
  public void testListGlobberDeepLevel1() throws Exception {
    FileStatus[] paths;
    sConfFlatListing.put("fs.cos.flat.list", "true");
    createCOSFileSystem(sConfFlatListing);
    paths = sFileSystem.globStatus(new Path(sBaseURI + "/tmp/manyHivePartitions/*"));
    assertEquals(dumpStats(sBaseURI + "/tmp/manyHivePartitions/*", paths), 4, paths.length);
  }

  @Test
  public void testListGlobberDeepLevel2() throws Exception {
    FileStatus[] paths;
    sConfFlatListing.put("fs.cos.flat.list", "true");
    createCOSFileSystem(sConfFlatListing);
    paths = sFileSystem.listStatus(new Path(sBaseURI
        + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/"));
    assertEquals(dumpStats(sBaseURI
        + "/tmp/manyHivePartitions/log_write_time=2019-05-09-12/*", paths), 2, paths.length);
  }

}
