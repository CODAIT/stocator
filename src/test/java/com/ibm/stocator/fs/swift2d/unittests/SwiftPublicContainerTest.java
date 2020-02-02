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

package com.ibm.stocator.fs.swift2d.unittests;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import com.ibm.stocator.fs.ObjectStoreFileSystem;

public class SwiftPublicContainerTest {

  private static final String PUBLIC_URI_PROPERTY = "fs.swift2d.public.uri";

  // @Test
  public void accessPublicSwiftContainerTest() throws Exception {

    FileSystem fs = new ObjectStoreFileSystem();
    Configuration conf = new Configuration();
    String uriString = conf.get(PUBLIC_URI_PROPERTY);
    Assume.assumeNotNull(uriString);
    URI publicContainerURI = new URI(uriString);
    System.out.println("publicContainerURI1: " + publicContainerURI);
    fs.initialize(publicContainerURI, conf);
    FileStatus objectFS = null;
    try {
      objectFS = fs.getFileStatus(new Path(publicContainerURI));
    } finally {
      Assert.assertNotNull("Unable to access public object.", objectFS);
    }
  }

}
