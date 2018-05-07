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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  com.ibm.stocator.fs.common.FileSystemTestUtils;
import com.ibm.stocator.fs.ObjectStoreFileSystem;

/**
 * This is the base class for most of the Swift tests
 */
public class SwiftBaseTest extends Assert {

  protected static final Logger LOG = LoggerFactory.getLogger(SwiftBaseTest.class);
  protected static ObjectStoreFileSystem sFileSystem;
  protected static String sBaseURI;
  private static final String BASE_SWIFT_URI_PROPERTY = "fs.swift2d.test.uri";
  private static Configuration sConf;

  @Before
  public void setUp() throws Exception {
    Assume.assumeNotNull(sFileSystem);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    createSwiftFileSystem();
  }

  public void manualSetUp(String containerName) throws Exception {
    createSwiftFileSystem(containerName);
  }

  public static void createSwiftFileSystem() throws Exception {
    createSwiftFileSystem("");
  }

  public static void createSwiftFileSystem(String containerName) throws Exception {
    sConf = new Configuration();
    sBaseURI = sConf.get(BASE_SWIFT_URI_PROPERTY);
    if (sBaseURI == null || sBaseURI.equals("")) {
      return;
    }

    if (!containerName.isEmpty()) {
      sBaseURI = sBaseURI.replace(sBaseURI.substring(sBaseURI.indexOf("//") + 2,
              sBaseURI.indexOf(".")), containerName);
      System.out.println("New uri is " + sBaseURI);
    }

    final URI uri = new URI(sBaseURI);
    sFileSystem = new ObjectStoreFileSystem();
    try {
      sFileSystem.initialize(uri, sConf);
    } catch (Exception e) {
      sFileSystem = null;
      throw e;
    }
  }

  @After
  public void tearDown() throws Exception {

  }

  @AfterClass
  public static void classTearDown() throws Exception {
    FileSystemTestUtils.cleanupAllFiles(sFileSystem, sBaseURI);
  }

  /**
   * Get the configuration used to set up the FS
   *
   * @return the configuration
   */
  public Configuration getConf() {
    return sConf;
  }

  protected int getBlockSize() {
    return 1024;
  }

  /**
   * Get the filesystem
   *
   * @return the current FS
   */
  public ObjectStoreFileSystem getFs() {
    return sFileSystem;
  }

  public String getBaseURI() {
    return sBaseURI;
  }

  /**
   * Create a file with the given data.
   *
   * @param path path to write
   * @param sourceData source dataset
   * @throws IOException on any problem
   */

  protected static void createFile(Path path, byte[] sourceData) throws IOException {
    if (sFileSystem != null) {
      System.out.println("Create " + path.toString());
      FSDataOutputStream out = sFileSystem.create(path);
      out.write(sourceData, 0, sourceData.length);
      out.close();
    }
  }

  /**
   * Create and then close a file
   *
   * @param path path to create
   * @throws IOException on a failure
   */
  protected static void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = sFileSystem.create(path);
    out.close();
  }

}
