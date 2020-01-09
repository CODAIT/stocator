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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Globber operations on the data that was not created by Stocator
 */
public class TestEmptyObjectFlatListing extends COSFileSystemBaseTest {

  private static Path[] sTestData;
  private static Path[] sEmptyFiles;
  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConf = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConf.put("fs.cos.flat.list", "false");
    createCOSFileSystem(sConf);
    if (sFileSystem != null) {
      createTestData();
    }
  }

  private static void createTestData() throws IOException {

    sTestData = new Path[] {
        new Path(sBaseURI + "/test1/year=2012/month=1/data.csv"),
        new Path(sBaseURI + "/test1/year=2012/month=10/data.csv"),
        new Path(sBaseURI + "/test1/year=2012/month=11/data.csv")};

    sEmptyFiles = new Path[] {
        new Path(sBaseURI + "/test1/year=2012/month=10")};

    for (Path path : sTestData) {
      createFile(path, sData);
    }
    for (Path path : sEmptyFiles) {
      createEmptyFile(path);
    }

  }

  @Test
  public void testListGlobber() throws Exception {
    Path lPath = new Path(sBaseURI + "/test1/year=2012/");
    FileStatus[] res = sFileSystem.listStatus(lPath);
    System.out.println("Stocator returned list of size: " + res.length);
    // Stocator doesn't list empty objects so we expect the output to be only the three directories
    for (FileStatus fs: res) {
      System.out.println("Stocator" + fs.getPath() + " directory " + fs.isDirectory());
      assertEquals(fs.isDirectory(), true);
    }
    assertEquals(res.length, 3);
  }
}
