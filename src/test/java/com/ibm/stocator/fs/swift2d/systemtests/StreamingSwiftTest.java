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

package com.ibm.stocator.fs.swift2d.systemtests;

import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.ibm.stocator.fs.ObjectStoreFileSystem;

import static com.ibm.stocator.fs.common.Utils.getHost;

public class StreamingSwiftTest {

  private boolean objectExpired = false;

  private void createObjectTimer(final double time, final String file) {

    // This thread handles the timePerFile expiration policy
    // This thread sleeps for the time specified.
    // When it wakes up, it will set the file as expired and close it
    // When the next tuple comes in, we check that the file has
    // expired and will create a new file for writing
    Thread fObjectTimerThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          System.out.println("Object Timer Started: " + time);
          Thread.sleep((long) time);
          objectTimerExpired(file);
        } catch (Exception e) {
          System.out.println("Exception in object timer thread: " + e.getMessage());
        }
      }

    });
    fObjectTimerThread.setDaemon(false);
    fObjectTimerThread.start();
  }

  public synchronized void objectTimerExpired(String file) {
    System.out.println("Timer Expired!!!");
    objectExpired = true;
  }

  // @Test
  public void accessPublicSwiftContainerWithSpaceTest() throws Exception {
    FileSystem fs = new ObjectStoreFileSystem();
    Configuration conf = new Configuration();
    String uriString = conf.get("fs.swift2d.test.uri");
    Assume.assumeNotNull(uriString);
    // adding suffix with space to the container name
    String scheme = "swift2d";
    String host = getHost(URI.create(uriString));
    // String origContainerName = getContainerName(host);
    // String newContainerName = origContainerName + " t";
    // uriString = uriString.replace(origContainerName, newContainerName);
    // use URI ctor that encodes authority according to the rules specified
    // in RFC 2396, section 5.2, step 7
    URI publicContainerURI = new URI(scheme, getHost(URI.create(uriString)), "/", null, null);
    fs.initialize(publicContainerURI, conf);
    FileStatus objectFS = null;
    try {
      objectFS = fs.getFileStatus(new Path(publicContainerURI));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertNotNull("Unable to access public object ", objectFS);
    }
  }

  @Test
  public void accessObjectWithSpaceTest() throws Exception {
    FileSystem fs = new ObjectStoreFileSystem();
    Configuration conf = new Configuration();
    String uriString = conf.get("fs.swift2d.test.uri");
    Assume.assumeNotNull(uriString);
    // adding suffix with space to the container name
    String scheme = "swift2d";
    String objectName = "/a/testObject.txt";
    URI publicContainerURI = new URI(uriString + objectName);
    // initialize file system
    fs.initialize(publicContainerURI, conf);
    FileStatus objectFS = null;
    Path f = null;
    try {
      FSDataOutputStream fsDataOutputStream = null;
      String currObjName = null;
      for (int i = 0; i < 5; i++) {
        currObjName = objectName + String.valueOf(i);
        // create timer
        createObjectTimer(90000.0, currObjName);
        publicContainerURI = new URI(scheme + "://"
            + getHost(URI.create(uriString)) + "/" + currObjName);
        f = new Path(publicContainerURI.toString());
        fsDataOutputStream = fs.create(f);
        String line = null;
        while (!objectExpired) {
          // generates input
          byte[] bytes = new byte[0];
          line = "\"2017-7-15 3:6:43\"," + String.valueOf(Math.random()) + ",6,18" + "\n";
          ByteBuffer linesBB = ByteBuffer.wrap(line.getBytes());
          bytes = new byte[linesBB.limit()];
          linesBB.get(bytes);

          // writes to output
          fsDataOutputStream.write(bytes);

          // simulate delays in input
          Thread.sleep(50);
        }
        fsDataOutputStream.close();
        objectExpired = false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertNotNull("Unable to access public object.", objectFS);
    } finally {
      fs.delete(f, true);
    }
  }

}
