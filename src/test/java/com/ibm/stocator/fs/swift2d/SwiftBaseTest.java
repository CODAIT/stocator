/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.swift2d;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.ObjectStoreFileSystem;

/**
 * This is the base class for most of the Swift tests
 */
public class SwiftBaseTest extends Assert {

  protected static final Logger LOG = LoggerFactory.getLogger(SwiftBaseTest.class);
  protected ObjectStoreFileSystem fs;
  protected String baseURI;
  private static final String BASE_URI_PROPERTY = "fs.swift2d.test.uri";
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    baseURI = conf.get(BASE_URI_PROPERTY);
    if (baseURI == null || baseURI.equals("")) {
      return;
    }
    final URI uri = new URI(baseURI);
    fs = new ObjectStoreFileSystem();
    try {
      fs.initialize(uri, conf);
    } catch (Exception e) {
      fs = null;
      throw e;
    }
  }

  public void manualSetUp(String containerName) throws Exception {
    conf = new Configuration();
    baseURI = conf.get(BASE_URI_PROPERTY);
    if (baseURI == null || baseURI.equals("")) {
      return;
    }
    System.out.println("container name is " + containerName);
    baseURI = baseURI.replace(baseURI.substring(baseURI.indexOf("//") + 2,
        baseURI.indexOf(".")), containerName);
    System.out.println("New uri is " + baseURI);
    final URI uri = new URI(baseURI);
    fs = new ObjectStoreFileSystem();
    try {
      fs.initialize(uri, conf);
    } catch (Exception e) {
      fs = null;
      throw e;
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @AfterClass
  public static void classTearDown() throws Exception {
  }

  /**
   * Get the configuration used to set up the FS
   *
   * @return the configuration
   */
  public Configuration getConf() {
    return conf;
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
    return fs;
  }

  public String getBaseURI() {
    return baseURI;
  }

  /**
   * Create a file with the given data.
   *
   * @param path path to write
   * @param sourceData source dataset
   * @throws IOException on any problem
   */
  protected void createFile(Path path, byte[] sourceData) throws IOException {
    System.out.print(".");
    FSDataOutputStream out = fs.create(path);
    out.write(sourceData, 0, sourceData.length);
    out.close();
  }

  /**
   * Create and then close a file
   *
   * @param path path to create
   * @throws IOException on a failure
   */
  protected void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = fs.create(path);
    out.close();
  }

}
