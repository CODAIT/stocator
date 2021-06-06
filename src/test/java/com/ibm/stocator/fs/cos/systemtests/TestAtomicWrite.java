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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test Atomic write using cos conditional requests
 */
@RunWith(Parameterized.class)
public class TestAtomicWrite extends COSFileSystemBaseTest {
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

    try {
      createFile(p, secondFile.getBytes(), false);
    } catch (IOException e) {
      assert (e.getMessage().contains(
              "At least one of the preconditions you specified did not hold"));
    }
  }

  /**
   * Checks writing an object with overwrite == true when the destination object doesn't exist
   * at the time of the request.
   * If in between another writing succeeded the write operation should fail as it
   * assumed that the object doesn't exist.
   * The If-None-Match should be set to * preventing the first write operation from succeeding
   * @throws Exception
   */
  @Test
  public void testIfNonMatchObjectNotExists() throws Exception {
    Path p = new Path(sBaseURI + "/atomicwrite_fastupload_"
            + fastUpload + "_multipart_" + multiPart + "/b.txt");
    // create a request that will try to overwrite this object
    // the object doesn't exist yet so If-None-Match: * should be used
    // the write doesn't finish at this stage as close on the stream was not called yet
    FSDataOutputStream out = sFileSystem.create(p, true);
    String firstFile = "Some text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      firstFile += createDataSize(1024 * 8);
    }
    out.write(firstFile.getBytes(), 0, firstFile.length());

    // create an object in between that is being written to storage
    String secondFile = "Some new text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      secondFile += createDataSize(1024 * 8);
    }
    createFile(p, secondFile.getBytes());

    // close the request for the first write
    // this should fail because a new object was written in between
    try {
      out.close();
    } catch (IOException e) {
      assert (e.getMessage().contains(
              "At least one of the preconditions you specified did not hold"));
    }
  }

  /**
   * Checks writing an object with overwrite == true when the destination object already exists at
   * the time of writing.
   * If in between another writing succeeded in overwriting the destination object the
   * write operation should fail as it assumed it is overwriting the original destination object.
   * The If-Match should be set to the original destination object Etag preventing the first write
   * operation from succeeding
   * @throws Exception
   */
  @Test
  public void testIfMatchObjectExists() throws Exception {
    Path p = new Path(sBaseURI + "/atomicwrite_fastupload_"
            + fastUpload + "_multipart_" + multiPart + "/c.txt");
    // create an object
    String firstFile = "Some text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      firstFile += createDataSize(1024 * 8);
    }
    createFile(p, firstFile.getBytes());

    // create a request that will try to overwrite this object
    // the destination object exists so an If-Match header with the object Etag should be used
    // the write doesn't finish at this stage as close on the stream was not called yet
    FSDataOutputStream out = sFileSystem.create(p, true);
    String secondFile = "Some between text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      secondFile += createDataSize(1024 * 8);
    }
    out.write(secondFile.getBytes(), 0, secondFile.length());

    // overwrite the original object in between so its Etag changes
    String thirdFile = "Some new text";
    if (multiPart) {
      // create large enough file to trigger multipart upload
      thirdFile += createDataSize(1024 * 8);
    }
    createFile(p, thirdFile.getBytes());

    // close the request for the overwrite object
    try {
      out.close();
    } catch (IOException e) {
      assert (e.getMessage().contains(
              "At least one of the preconditions you specified did not hold"));
    }
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
