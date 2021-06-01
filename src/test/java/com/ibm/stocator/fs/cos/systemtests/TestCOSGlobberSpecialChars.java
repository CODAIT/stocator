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
import static com.ibm.stocator.fs.common.FileSystemTestUtils.dumpStats;

/**
 * Test the FileSystem#listStatus() operations
 */
public class TestCOSGlobberSpecialChars extends COSFileSystemBaseTest {

  private static Path[] sTestData;
  private static Path[] sEmptyFiles;
  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConf = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConf.put("fs.cos.flat.list", "true");
    createCOSFileSystem(sConf);
    if (sFileSystem != null) {
      createTestData();
    }
  }

  private static void createTestData() throws IOException {

    sTestData = new Path[] {
        new Path(sBaseURI + "/test/val=a.b/y=2018/m=10/d=29.a/data2json/"
            + "part-00000-9e959568-1cc5-4bc6-966d-9b366be2204c.json"),
        new Path(sBaseURI + "/test/val=a.b/y=2018/m=10/d=29.a/data2json/"
            + "part-00001-9e959568-1cc5-4bc6-966d-9b366be2204c.json")};

    sEmptyFiles = new Path[] {
        new Path(sBaseURI + "/test/val=a.b/y=2018/m=10/d=29.a/data2json"),
        new Path(sBaseURI + "/test/val=a.b/y=2018/m=10/d=29.a/data2json/_SUCCESS")};

    for (Path path : sTestData) {
      createFile(path, sData);
    }
    for (Path path : sEmptyFiles) {
      createEmptyFile(path);
    }
  }

  @Test
  public void testListGlobber() throws Exception {
    FileStatus[] paths;
    paths = sFileSystem.globStatus(new Path(getBaseURI(), "test/val=a.b/y=2018/*"));
    for (FileStatus path: paths) {
      System.out.println(path.getPath());
    }
    assertEquals(dumpStats("/test/*", paths), 2, paths.length);
  }

}
