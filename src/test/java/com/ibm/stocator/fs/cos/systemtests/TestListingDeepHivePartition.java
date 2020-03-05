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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test listing time deep hive style partitioning with and without flat listing turned on
 */
public class TestListingDeepHivePartition extends COSFileSystemBaseTest {

  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConfFlatListing = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConfFlatListing.put("fs.cos.flat.list", "false");
    createCOSFileSystem(sConfFlatListing);
    if (sFileSystem != null) {
      createTestData();
    }
  }

  private static void createTestData() throws IOException {
    // create files deep hive style partitioing
    for (int i = 0; i < 10; i++) {
      createFile(new Path(sBaseURI + "/testListingDeepHiveStyle/a=" + i
              + "/b=" + i + "/c=" + i + "/d=" + i + "/e=" + i + "/f" + i), sData);
    }
  }

  @Test
  public void testListGlobber() throws Exception {
    Path dataPath = new Path(sBaseURI + "/testListingDeepHiveStyle");

    // Check with flat listing turned off
    long startTime = System.nanoTime();
    List<FileStatus> flatListingRes = listRecursive(dataPath);
    long endTime = System.nanoTime();
    long listingTimeNonFlatListing = endTime - startTime;
    System.out.println("Stocator listing took for dataset starting with part "
            + listingTimeNonFlatListing + "ns");

    // Check now with flat listing turned on
    sConfFlatListing.put("fs.cos.flat.list", "true");
    createCOSFileSystem(sConfFlatListing);
    startTime = System.nanoTime();
    FileStatus[] nonFlatListingRes = sFileSystem.listStatus(dataPath);
    endTime = System.nanoTime();
    long listingTimeFlatListing = endTime - startTime;
    System.out.println("Stocator listing took for dataset starting with part "
            + listingTimeFlatListing + "ns");

    assertTrue("lists not equal", nonFlatListingRes.length == flatListingRes.size());
    assertFalse("Listing deep hive style partitioning took too long",
            listingTimeNonFlatListing > 2 * listingTimeFlatListing);
  }

  static List<FileStatus> listRecursive(Path path) throws IOException {
    List<FileStatus> res = new ArrayList<>();
    FileStatus[] lst = sFileSystem.listStatus(path);

    // list recursively
    for (FileStatus fileStatus: lst) {
      if (fileStatus.isDirectory()) {
        res.addAll(listRecursive(fileStatus.getPath()));
      } else {
        res.add(fileStatus);
      }
    }
    return res;
  }
}
