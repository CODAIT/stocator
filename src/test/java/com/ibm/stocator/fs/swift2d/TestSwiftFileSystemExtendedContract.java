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

package com.ibm.stocator.fs.swift2d;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import com.ibm.stocator.fs.ObjectStoreVisitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import com.ibm.stocator.fs.common.Constants;
import org.apache.hadoop.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

public class TestSwiftFileSystemExtendedContract extends SwiftFileSystemBaseTest {

  private static final String BASE_URI_PROPERTY = "fs.swift2d.test.uri";

  @Ignore("Unexpected")
  public void testOpenNonExistingFile() throws IOException {
    final Path p = new Path("/test/testOpenNonExistingFile");
    //open it as a file, should get FileNotFoundException
    try {
      final FSDataInputStream in = fs.open(p);
      in.close();
      fail("didn't expect to get here");
    } catch (FileNotFoundException fnfe) {
      LOG.debug("Expected: " + fnfe, fnfe);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testFilesystemHasURI() throws Throwable {
    assertNotNull(fs.getUri());
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testCreateFile() throws Exception {
    final Path f = new Path(getBaseURI() + "/test/testCreateFile");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    fsDataOutputStream.close();
    assertExists("created file", f);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testWriteReadFile() throws Exception {
    final Path f = new Path(getBaseURI() + "/test/test");
    final FSDataOutputStream fsDataOutputStream = fs.create(f);
    final String message = "Test string";
    fsDataOutputStream.write(message.getBytes());
    fsDataOutputStream.close();
    assertExists("created file", f);
    FSDataInputStream open = null;
    try {
      open = fs.open(f);
      final byte[] bytes = new byte[512];
      final int read = open.read(bytes);
      final byte[] buffer = new byte[read];
      System.arraycopy(bytes, 0, buffer, 0, read);
      assertEquals(message, new String(buffer));
    } finally {
      fs.delete(f, false);
      IOUtils.closeStream(open);
    }
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testConfDefinesFilesystem() throws Throwable {
    Configuration conf = new Configuration();
    baseURI = conf.get(BASE_URI_PROPERTY);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testConfIsValid() throws Throwable {
    Configuration conf = new Configuration();
    baseURI = conf.get(BASE_URI_PROPERTY);
    String nameSpace = baseURI.substring(0, baseURI.indexOf("://"));
    ObjectStoreVisitor.getStoreClient(nameSpace, new URI(baseURI), conf);
  }

  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testGetSchemeImplemented() throws Throwable {
    String scheme = fs.getScheme();
    assertEquals(Constants.SWIFT2D,scheme);
  }

  /**
   * Assert that a filesystem is case sensitive.
   * This is done by creating a mixed-case filename and asserting that
   * its lower case version is not there.
   *
   * @throws Exception failures
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testFilesystemIsCaseSensitive() throws Exception {
    String mixedCaseFilename = "/test/UPPER.TXT";
    Path upper = path(getBaseURI() + mixedCaseFilename);
    Path lower = path(getBaseURI() + mixedCaseFilename.toLowerCase());
    assertFalse("File exists" + upper, fs.exists(upper));
    assertFalse("File exists" + lower, fs.exists(lower));
    FSDataOutputStream out = fs.create(upper);
    out.writeUTF("UPPER");
    out.close();
    FileStatus upperStatus = fs.getFileStatus(upper);
    assertExists("Original upper case file" + upper, upper);
    //verify the lower-case version of the filename doesn't exist
    assertPathDoesNotExist("lower case file", lower);
    //now overwrite the lower case version of the filename with a
    //new version.
    out = fs.create(lower);
    out.writeUTF("l");
    out.close();
    assertExists("lower case file", lower);
    //verify the length of the upper file hasn't changed
    assertExists("Original upper case file " + upper, upper);
    FileStatus newStatus = fs.getFileStatus(upper);
    assertEquals("Expected status:" + upperStatus
            + " actual status " + newStatus,
            upperStatus.getLen(),
            newStatus.getLen());
  }

}
