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

import com.ibm.stocator.fs.ObjectStoreFileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;


import static com.ibm.stocator.fs.swift2d.SwiftTestUtils.noteAction;

/**
 * This is the base class for most of the Swift tests
 */
public class SwiftFileSystemBaseTest extends Assert implements
                                                    SwiftTestConstants {

  protected static final Log LOG =
          LogFactory.getLog(SwiftFileSystemBaseTest.class);
  protected ObjectStoreFileSystem fs;
  protected static ObjectStoreFileSystem lastFs;
  protected byte[] data = SwiftTestUtils.generateDataset(getBlockSize() * 2, 0, 255);
  private Configuration conf;
  protected String baseURI;
  private static final String BASE_URI_PROPERTY = "fs.swift2d.test.uri";

  @Before
  public void setUp() throws Exception {
    noteAction("setup");
    conf = new Configuration();
    baseURI = conf.get(BASE_URI_PROPERTY);
    if (baseURI == null || baseURI.equals("")) {
      return;
    }
    final URI uri = new URI(baseURI);
    conf = createConfiguration();

    fs = new ObjectStoreFileSystem();
    try {
      fs.initialize(uri, conf);
    } catch (IOException e) {
      //FS init failed, set it to null so that teardown doesn't
      //attempt to use it
      fs = null;
      throw e;
    }
    //remember the last FS
    lastFs = fs;
    noteAction("setup complete");
  }

  /**
   * Configuration generator. May be overridden to inject
   * some custom options
   * @return a configuration with which to create FS instances
   */
  protected Configuration createConfiguration() {
    return new Configuration();
  }

  @After
  public void tearDown() throws Exception {
    if (getBaseURI() != null) {
      // Clean up generated files
      Path rootDir = new Path(getBaseURI());
      FileStatus[] files = getFs().listStatus(rootDir);
      for (FileStatus file : files) {
        getFs().delete(file.getPath(), false);
      }
    }
  }

  @AfterClass
  public static void classTearDown() throws Exception {
  }

  /**
   * Get the configuration used to set up the FS
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Describe the test, combining some logging with details
   * for people reading the code
   *
   * @param description test description
   */
  protected void describe(String description) {
    noteAction(description);
  }

  protected int getBlockSize() {
    return 1024;
  }

  /**
   * Take an unqualified path, and qualify it w.r.t the
   * current filesystem
   * @param pathString source path
   * @return a qualified path instance
   */
  protected Path path(String pathString) {
    return new Path(pathString).makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * Get the filesystem
   * @return the current FS
   */
  public ObjectStoreFileSystem getFs() {
    return fs;
  }

  /**
   * Create a file using the standard {@link #data} bytes.
   *
   * @param path path to write
   * @throws IOException on any problem
   */
  protected void createFile(Path path) throws IOException {
    createFile(path, data);
  }

  /**
   * Create a file with the given data.
   *
   * @param path       path to write
   * @param sourceData source dataset
   * @throws IOException on any problem
   */
  protected void createFile(Path path, byte[] sourceData) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.write(sourceData, 0, sourceData.length);
    out.close();
  }

  /**
   * Create and then close a file
   * @param path path to create
   * @throws IOException on a failure
   */
  protected void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.close();
  }


  /**
   * assert that a path exists
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertExists(String message, Path path) throws IOException {
    SwiftTestUtils.assertPathExists(fs, message, path);
  }

  /**
   * assert that a path does not
   * @param message message to use in an assertion
   * @param path path to probe
   * @throws IOException IO problems
   */
  public void assertPathDoesNotExist(String message, Path path) throws
          IOException {
    SwiftTestUtils.assertPathDoesNotExist(fs, message, path);
  }

  /**
   * Assert that a file exists and whose {@link FileStatus} entry
   * declares that this is a file and not a symlink or directory.
   *
   * @throws IOException IO problems during file operations
   */
  protected void mkdirs(Path path) throws IOException {
    assertTrue("Failed to mkdir" + path, fs.mkdirs(path));
  }

  /**
   * Assert that a delete succeeded
   * @param path path to delete
   * @param recursive recursive flag
   * @throws IOException IO problems
   */
  protected void assertDeleted(Path path, boolean recursive) throws IOException {
    SwiftTestUtils.assertDeleted(fs, path, recursive);
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

  public String getBaseURI() {
    return baseURI;
  }
}
