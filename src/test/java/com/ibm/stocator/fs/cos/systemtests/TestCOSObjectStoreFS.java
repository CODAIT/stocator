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

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test Globber operations on the data that was not created by Stocator
 */
public class TestCOSObjectStoreFS extends COSFileSystemBaseTest {

  private static byte[] sData = "This is file".getBytes();
  private static Hashtable<String, String> sConf = new Hashtable<String, String>();

  @BeforeClass
  public static void setUpClass() throws Exception {
    sConf.put("fs.stocator.glob.bracket.support", "true");
    createCOSFileSystem(sConf);
  }

  @Test
  public void testMkdirs() throws Exception {
    Path p = new Path(getBaseURI(), "a/_spark_metadata");
    sFileSystem.mkdirs(p);
    boolean res = sFileSystem.exists(p);
    assertEquals("_spark_metdata directory was not created", true, res);
    Path tmpFilePath = new Path(getBaseURI(),
        "a/_spark_metadata/.6109afea-c983-4748-bebb-d7e05d2f46f8.tmp");
    createFile(tmpFilePath, sData);
    res = sFileSystem.exists(tmpFilePath);
    assertEquals(tmpFilePath + " failed to created", true, res);
    Path dstFilePath = new Path(getBaseURI(), "a/_spark_metadata/3");
    sFileSystem.rename(tmpFilePath, dstFilePath);
    res = sFileSystem.exists(tmpFilePath);
    assertEquals(tmpFilePath + " failed to delete after copy", false, res);
    res = sFileSystem.exists(dstFilePath);
    assertEquals(dstFilePath + " failed to copy into final name", true, res);
  }

}
