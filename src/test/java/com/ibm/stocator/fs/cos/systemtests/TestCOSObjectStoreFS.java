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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Globber operations on the data that was not created by Stocator
 */
public class TestCOSObjectStoreFS extends COSFileSystemBaseTest {

  private static Path[] sTestData;
  private static Path[] sEmptyFiles;
  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConf = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConf.put("fs.stocator.glob.bracket.support", "true");
    createCOSFileSystem(sConf);
  }

  @Test
  public void testMkdirs() throws Exception {
    FileStatus[] paths;
    Path p = new Path(getBaseURI(), "a/_spark_metadata");
    sFileSystem.mkdirs(p);
    FileStatus[] res = sFileSystem.listStatus(p);
  }

}
