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

package com.ibm.stocator.fs.swift2d.systemtests;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.common.FileSystemTestUtils;
import com.ibm.stocator.fs.common.TestConstants;

/**
 * Test deletion operations
 */
public class TestSwiftFileSystemDelete extends SwiftFileSystemBaseTest {

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDeleteEmptyFile() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteEmptyFile");
    createEmptyFile(file);
    FileSystemTestUtils.noteAction("about to delete");
    assertDeleted(file, true);
  }

  @Ignore("Unexpected")
  public void testDeleteEmptyFileTwice() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteEmptyFileTwice");
    createEmptyFile(file);
    assertDeleted(file, true);
    FileSystemTestUtils.noteAction("multiple creates, and deletes");
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
    createEmptyFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDeleteNonEmptyFile() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteNonEmptyFile");
    createFile(file);
    assertDeleted(file, true);
  }

  @Ignore("Unexpected")
  public void testDeleteNonEmptyFileTwice() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/testDeleteNonEmptyFileTwice");
    createFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
    createFile(file);
    assertDeleted(file, true);
    assertFalse("Delete returned true", sFileSystem.delete(file, false));
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDeleteTestDir() throws IOException {
    final Path file = new Path(getBaseURI() + "/test/");
    sFileSystem.delete(file, true);
    assertPathDoesNotExist("Test dir found", file);
  }

  /**
   * Test recursive root directory deletion fails if there is an entry underneath
   * @throws Throwable
   */
  @Ignore("Not supported")
  public void testRmRootDirRecursiveIsForbidden() throws Throwable {
    Path root = path(getBaseURI() + "/");
    Path testFile = path(getBaseURI() + "/test");
    createFile(testFile);
    assertTrue("rm(/) returned false", sFileSystem.delete(root, true));
    assertExists("Root dir is missing", root);
    assertPathDoesNotExist("test file not deleted", testFile);
  }

}
