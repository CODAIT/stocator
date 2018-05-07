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

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;

/**
 * Test the FileSystem#listStatus() operations
 */
public class TestCOSGlobber extends COSFileSystemBaseTest {

  private static Path[] sTestData;
  private static byte[] sData = "This is file".getBytes();

  @BeforeClass
  public static void setUpClass() throws Exception {
    createCOSFileSystem();
    if (sFileSystem != null) {
      createTestData();
    }
  }

  private static void createTestData() throws IOException {

    sTestData = new Path[]{ new Path(sBaseURI + "/test/y=12/a"),
                            new Path(sBaseURI + "/test/y=14/b")};
    for (Path path : sTestData) {
      createFile(path, sData);
    }
  }

}
