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
import java.util.Arrays;
import java.util.Collection;
import java.util.Hashtable;

import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test Atomic write using cos conditional requests
 */
@RunWith(Parameterized.class)
public class TestAtomicWrite extends COSFileSystemBaseTest {
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private Hashtable<String, String> sConf = new Hashtable<String, String>();
  private Boolean fastUpload;
  private Boolean multiPart;

  @Parameterized.Parameters(name = "fastUpload = {0}, multiPart = {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
            // fastUpload = false, multiPart = false
            { false, false },
            // fastUpload = false, multiPart = true
            { false, true },
            // fastUpload = true, multiPart = false
            { true , false },
            // fastUpload = true, multiPart = true
            { true , true }
    });
  }

  public TestAtomicWrite(boolean fstUpload, boolean mltPart) throws Exception {
    fastUpload = fstUpload;
    multiPart = mltPart;
    sConf.put("fs.cos.atomic.write", "true");
    sConf.put("fs.cos.fast.upload", Boolean.toString(fastUpload));
    // tune multipart configuration to a smaller size for the test
    if (multiPart) {
      sConf.put("fs.cos.multipart.threshold", Integer.toString(5 * 1024 * 1024)); // 5MB
      sConf.put("fs.cos.multipart.size", Integer.toString(5 * 1024 * 1024)); // 5MB
    }
    createCOSFileSystem(sConf);
  }

  /**
   * Checks writing an object when overwrite == false
   * In this case the write should fail if an object already exists at the destination
   * The If-None-Match should be set to * preventing the write from succeeding
   * @throws Exception
   */
  @Test
  public void testIfNoneMatchObjectExists() throws Exception {
    Path p = new Path(sBaseURI + "/atomicwrite_fastupload_"
            + fastUpload + "_multipart_" + multiPart + "/a.txt");
    // create the object without overwrite
    String firstFile = "Some text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      firstFile += createDataSize(1024 * 8);
    }
    createFile(p, firstFile.getBytes(), false);

    // Now try to create the same file with different content again without overwrite
    // should fail as a file already exists
    String secondFile = "Some new text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      secondFile += createDataSize(1024 * 8);
    }

    exceptionRule.expect(IOException.class);
    exceptionRule.expectMessage("At least one of the preconditions you specified did not hold");
    createFile(p, secondFile.getBytes(), false);
  }

  /**
   * Creates a string of size @msgSize in KB.
   */
  private static String createDataSize(int msgSize) {
    // Java chars are 2 bytes but we don't need to divide by 2 since
    // only 1 byte is used for common characters in string encoding
    msgSize = msgSize * 1024;
    StringBuilder sb = new StringBuilder(msgSize);
    for (int i = 0; i < msgSize; i++) {
      sb.append('a');
    }
    return sb.toString();
  }
}
