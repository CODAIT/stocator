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

package  com.ibm.stocator.fs.swift2d;

import java.io.IOException;
import java.text.MessageFormat;

import com.ibm.stocator.fs.common.ObjectStoreGlobFilter;
import com.ibm.stocator.fs.common.ObjectStoreGlobber;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestSwiftOperations extends SwiftBaseTest {

  protected byte[] data = SwiftTestUtils.generateDataset(getBlockSize() * 2, 0, 255);
  // {1}, {3} = two digits, starting with 00
  private String sparkPutFormat = "/{0}/_temporary/0/_temporary/"
      + "attempt_201612062056_0000_m_0000{1}_{2}/part-000{3}";
  // {1} = two digits number, starting wih 00
  private String swiftDataFormat = "/{0}/part-000{3}-attempt_201612062056_0000_m_0000{1}_{2}";
  private String sparkSuccessFormat = "/{0}/_SUCCESS";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeNotNull(getFs());
  }

  @Test
  public void testDataObject() throws Exception {
    String objectName = "data7.txt";
    Object[] params;
    // create 11 objects
    for (int i = 0;i < 11; i++) {
      String id = String.format("%0" + 2 + "d", i);
      params = new Object[]{objectName, id, String.valueOf(i), id};
      Path path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, data);
    }
    // create _SUCCESS object
    createEmptyFile(new Path(getBaseURI(),
        MessageFormat.format(sparkSuccessFormat, new Object[]{objectName})));
    FileStatus[]  stats = getFs().listStatus(new Path(getBaseURI() + "/" + objectName));
    Assert.assertTrue(11 == stats.length);
    // read 11 objects
    for (int i = 0;i < 11; i++) {
      String id = String.format("%0" + 2 + "d", i);
      params = new Object[]{objectName, id, String.valueOf(i), id};
      Path path = new Path(getBaseURI(), MessageFormat.format(swiftDataFormat, params));
      System.out.print(".");
      byte[] res = SwiftTestUtils.readDataset(getFs(),
          path, data.length);
      Assert.assertArrayEquals(data, res);
    }
    // delete 11 objects
    for (int i = 0;i < 11; i++) {
      String id = String.format("%0" + 2 + "d", i);
      params = new Object[]{objectName, id, String.valueOf(i), id};
      Path path = new Path(getBaseURI(), MessageFormat.format(swiftDataFormat, params));
      System.out.print(".");
      getFs().delete(path, false);
    }
    // delete _SUCCESS object
    getFs().delete(new Path(getBaseURI(),
            MessageFormat.format(sparkSuccessFormat, new Object[]{objectName})), false);
    stats = getFs().listStatus(new Path(getBaseURI() + "/" + objectName));
    Assert.assertTrue(0 == stats.length);
  }

  @Test
  public void testCopy() throws Exception {
    Assume.assumeNotNull(getFs());
    String object = "file1";
    Path source = new Path(getBaseURI() + "/" + object);
    createFile(source, data);
    Path destination = new Path(getBaseURI() + "/" + "copied");

    try {
      //Check copy operation returns true
      Assert.assertTrue(getFs().copy(source, destination));

      // Check copied file exists
      Assert.assertTrue(getFs().exists(destination));
    } finally {
      getFs().delete(source, false);
      getFs().delete(destination, false);
    }
  }

  @Test
  public void testCopyDirectory() throws Exception {
    Assume.assumeNotNull(getFs());
    String dirName = "Dir1";
    String[] objects = {"file1", "file2", "subDirB/file3"};
    Path sourceDir = new Path(getBaseURI() + "/" + dirName);
    createEmptyFile(sourceDir);
    for (String object : objects) {
      Path source = new Path(sourceDir.toString() + "/" + object);
      createFile(source, data);
    }
    Path destinationDir = new Path(getBaseURI() + "/" + "copied");
    try {
      //Check copy operation returns true
      Assert.assertTrue(getFs().copy(sourceDir, destinationDir));

      for (String object : objects) {
        Path dst = new Path(destinationDir.toString() + "/" + object);
        // Check copied files exists
        Assert.assertTrue(getFs().exists(dst));
      }
    } finally {
      for (String object : objects) {
        getFs().delete(new Path(sourceDir.toString() + "/" + object), false);
        getFs().delete(new Path(destinationDir.toString() + "/" + object), false);
      }
      getFs().delete(sourceDir, false);
      getFs().delete(destinationDir, false);
    }
  }

  @Test
  public void testCopyTemp() throws Exception {
    Assume.assumeNotNull(getFs());
    String object = "file1/_temporary";
    Path source = new Path(getBaseURI() + "/" + object);
    createFile(source, data);
    Path destination = new Path(getBaseURI() + "/" + "renamed");

    try {
      // Check copy operation returns true
      Assert.assertTrue(getFs().copy(source, destination));
      // Check _temporary file still exists and is not copied
      Assert.assertTrue(getFs().exists(source));
      Assert.assertFalse(getFs().exists(destination));
    } finally {
      getFs().delete(source, false);
      getFs().delete(destination, false);
    }
  }

  @Test
  public void testCopyDifferentContainers() throws Exception {
    Assume.assumeNotNull(getFs());
    String object = "file1";
    Path source = new Path(getBaseURI() + "/" + object);
    createFile(source, data);
    Path destination = new Path("swift2d://testContainer.bmv3/renamed");

    // Check copy operation returns true
    Assert.assertTrue(getFs().copy(source, destination));
    try {
      Assert.assertTrue(getFs().exists(source));
    } finally {
      getFs().delete(source, true);
      getFs().delete(destination, true);
    }
  }

  @Test
  public void testFileExists() throws IOException {
    Path testFile = new Path(getBaseURI() + "/testFile");
    createFile(testFile, data);
    Assert.assertTrue(getFs().exists(testFile));
    getFs().delete(testFile, false);
    FileStatus[]  stats = getFs().listStatus(testFile);
    Assert.assertTrue(0 == stats.length);
  }

  @Test
  public void testListStatus() throws IOException {
    String[] testFileNames = {"/FileA", "/FileB", "/Dir/FileC"};
    for (String name : testFileNames) {
      createFile(new Path(getBaseURI() + name), data);
    }
    FileStatus[] results = getFs().listStatus(new Path(getBaseURI()));
    Assert.assertEquals(testFileNames.length, results.length);
    for (String name : testFileNames) {
      getFs().delete(new Path(getBaseURI() + name), false);
    }
    results = getFs().listStatus(new Path(getBaseURI()));
    Assert.assertTrue(0 == results.length);
  }

  @Test
  public void testAsteriskWildcard() throws Exception {
    String[] objectNames = {"Dir/SubDir/File1", "Dir/SubDir/File2", "Dir/File1"};
    for (String name : objectNames) {
      Path path = new Path(getBaseURI() + "/" + name);
      createFile(path, data);
    }

    Path wildcard = new Path(getBaseURI() + "/*"); // All files
    ObjectStoreGlobber globber = new ObjectStoreGlobber(getFs(), wildcard,
            new ObjectStoreGlobFilter(""));
    FileStatus[] results = globber.glob();
    Assert.assertEquals(3, results.length);

    wildcard = new Path(getBaseURI() + "/Dir/*"); // Files in "Dir" directory
    globber = new ObjectStoreGlobber(getFs(), wildcard, new ObjectStoreGlobFilter(""));
    results = globber.glob();
    Assert.assertEquals(3, results.length);

    wildcard = new Path(getBaseURI() + "/Dir/SubDir/*"); // Files in "SubDir" directory
    globber = new ObjectStoreGlobber(getFs(), wildcard, new ObjectStoreGlobFilter(""));
    results = globber.glob();
    Assert.assertEquals(2, results.length);

    wildcard = new Path(getBaseURI() + "/*1"); // Files ending in "1"
    globber = new ObjectStoreGlobber(getFs(), wildcard, new ObjectStoreGlobFilter(""));
    results = globber.glob();
    Assert.assertEquals(2, results.length);

    wildcard = new Path(getBaseURI() + "/Dir/SubDir/*2"); // Files in "SubDir" ending with "2"
    globber = new ObjectStoreGlobber(getFs(), wildcard, new ObjectStoreGlobFilter(""));
    results = globber.glob();
    Assert.assertEquals(1, results.length);

    wildcard = new Path(getBaseURI() + "/Dir/*/File1"); // Files called File1 in a SubDir
    globber = new ObjectStoreGlobber(getFs(), wildcard, new ObjectStoreGlobFilter(""));
    results = globber.glob();
    Assert.assertEquals(1, results.length);

  }
}
