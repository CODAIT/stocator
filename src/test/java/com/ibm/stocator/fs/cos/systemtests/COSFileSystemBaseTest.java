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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assume;

import com.ibm.stocator.fs.common.FileSystemTestUtils;

/**
 * This is the base class for most of the Swift tests
 */
public class COSFileSystemBaseTest extends COSBaseTest {

  protected byte[] data = FileSystemTestUtils.generateDataset(getBlockSize() * 2, 0, 255);

  @Override
  public void setUp() throws Exception {
    FileSystemTestUtils.noteAction("setup");
    super.setUp();
    Assume.assumeNotNull(getFs());
    FileSystemTestUtils.noteAction("setup complete");
  }

  /**
   * Describe the test, combining some logging with details
   * for people reading the code
   *
   * @param description test description
   */
  protected void describe(String description) {
    FileSystemTestUtils.noteAction(description);
  }

  /**
   * Take an unqualified path, and qualify it w.r.t the
   * current filesystem
   * @param pathString source path
   * @return a qualified path instance
   */
  protected Path path(String pathString) {
    return new Path(pathString);
  }

  /**
   * Create a file using the standard {@link #data} bytes.
   *
   * @param path path to write
   * @throws IOException on any problem
   */
  public void createFile(Path path) throws IOException {
    createFile(path, data);
  }

  /**
   * assert that a path exists
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertExists(String message, Path path) throws IOException {
    FileSystemTestUtils.assertPathExists(sFileSystem, message, path);
  }

  /**
   * assert that a path does not
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertPathDoesNotExist(String message, Path path) throws
          IOException {
    FileSystemTestUtils.assertPathDoesNotExist(sFileSystem, message, path);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @throws IOException IO problems during file operations
   */
  protected void mkdirs(Path path) throws IOException {
    createEmptyFile(path);
  }

  /**
   * Assert that a delete succeeded
   * @param path path to delete
   * @param recursive recursive flag
   * @throws IOException IO problems
   */
  protected void assertDeleted(Path path, boolean recursive) throws IOException {
    FileSystemTestUtils.assertDeleted(sFileSystem, path, recursive);
  }

  /**
   * Assert that a value is not equal to the expected value
   * @param message message if the two values are equal
   * @param expected expected value
   * @param actual actual value
   */
  protected void assertNotEqual(String message, int expected, int actual) {
    assertTrue(message,
               actual != expected);
  }
}
