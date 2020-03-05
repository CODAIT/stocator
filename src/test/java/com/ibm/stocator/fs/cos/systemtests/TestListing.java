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

import java.util.Hashtable;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Globber operations on the data that was not created by Stocator
 */
public class TestListing extends COSFileSystemBaseTest {

  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConf = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    createCOSFileSystem(sConf);
    if (sFileSystem != null) {
      createTestData();
    }
  }

  private static void createTestData() throws IOException {
    // create files without `part` prefix
    for (int i = 0; i < 10; i++) {
      createFile(new Path(sBaseURI + "/testListing/f" + i), sData);
    }
    // create files with `part` prefix
    for (int i = 0; i < 10; i++) {
      createFile(new Path(sBaseURI + "/testListingPart/part-" + i), sData);
    }
  }

  @Test
  public void testListGlobber() throws Exception {
    Path nonPartPath = new Path(sBaseURI + "/testListing");
    long startTime = System.nanoTime();
    sFileSystem.listStatus(nonPartPath);
    long endTime = System.nanoTime();
    long listingTimeNoPart = endTime - startTime;
    System.out.println("Stocator listing took for dataset not starting with part "
            + listingTimeNoPart + "ns");

    Path partPath = new Path(sBaseURI + "/testListingPart");
    startTime = System.nanoTime();
    sFileSystem.listStatus(partPath);
    endTime = System.nanoTime();
    long listingTimePart = endTime - startTime;
    System.out.println("Stocator listing took for dataset starting with part "
            + listingTimePart + "ns");

    assertFalse("Listing time without part is too long", listingTimeNoPart > 2 * listingTimePart);
  }
}
