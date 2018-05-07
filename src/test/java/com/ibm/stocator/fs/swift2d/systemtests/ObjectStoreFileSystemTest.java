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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import com.ibm.stocator.fs.ObjectStoreFileSystem;
import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.StocatorPath;
import com.ibm.stocator.fs.common.FileSystemTestUtils;

public class ObjectStoreFileSystemTest extends SwiftBaseTest {

  private ObjectStoreFileSystem mMockObjectStoreFileSystem;
  private StocatorPath mMockStocatorPath;
  private String hostName = "swift2d://out1003.lvm";
  private byte[] data = FileSystemTestUtils.generateDataset(getBlockSize() * 2, 0, 255);
  private String fileName = null;
  private int iterNum = 3;

  @Before
  public final void before() throws Exception {
    mMockObjectStoreFileSystem = PowerMockito.mock(ObjectStoreFileSystem.class);
    Whitebox.setInternalState(mMockObjectStoreFileSystem, "hostNameScheme", hostName);
    mMockStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mMockStocatorPath, "tempFileOriginator",
        Constants.DEFAULT_FOUTPUTCOMMITTER_V1);
    Whitebox.setInternalState(mMockStocatorPath, "tempIdentifier",
        Constants.HADOOP_TEMPORARY);

    if (getFs() != null) {
      int iterNum = 3;
      fileName = getBaseURI() + "/testFile";
      Path[] testFile0 = new Path[iterNum];
      for (int i = 0; i < iterNum; i++) {
        testFile0[i] = new Path(fileName + "0" + i);
        createFile(testFile0[i], data);
      }
      Path[] testFile1 = new Path[iterNum * 2];
      for (int i = 0; i < iterNum * 2; i++) {
        testFile1[i] = new Path(fileName + "1" + i);
        createFile(testFile1[i], data);
      }
    }
  }

  @After
  public final void after() throws Exception {
    if (getFs() != null) {
      fileName = getBaseURI() + "/testFile";
      Path[] testFile0 = new Path[iterNum];
      for (int i = 0; i < iterNum; i++) {
        testFile0[i] = new Path(fileName + "0" + i);
        getFs().delete(testFile0[i], false);
      }
      Path[] testFile1 = new Path[iterNum * 2];
      for (int i = 0; i < iterNum * 2; i++) {
        testFile1[i] = new Path(fileName + "1" + i);
        getFs().delete(testFile1[i], false);
      }
    }
  }

  @Test
  public void getObjectNameTest() throws Exception {
    Path input = new Path("swift2d://out1003.lvm/a/b/c/m.data/_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    String result = Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, hostName);
    Assert.assertEquals("/a/b/c/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

    input = new Path("swift2d://out1003.lvm/a/b/m.data/_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    result = Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, hostName);
    Assert.assertEquals("/a/b/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

    input = new Path("swift2d://out1003.lvm/m.data/_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    result = Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, hostName);
    Assert.assertEquals("/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

  }

  @Test(expected = IOException.class)
  public void getObjectWrongNameTest() throws Exception {
    Path input = new Path("swift2d://out1003.lvm_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, hostName);

    input = new Path("swift2d://out1003.lvm/temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, hostName);
  }

  @Test
  public void getSchemeTest() {
    Assume.assumeNotNull(getFs());
    Assert.assertEquals(Constants.SWIFT2D, getFs().getScheme());
  }

  @Test
  public void existsTest() throws Exception {
    Assume.assumeNotNull(getFs());
    Path testFile = new Path(getBaseURI() + "/testFile");
    getFs().delete(testFile, false);
    Assert.assertFalse(getFs().exists(testFile));

    createFile(testFile, data);
    Assert.assertTrue(getFs().exists(testFile));

    Path input = new Path(getBaseURI() + "/a/b/c/m.data/_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.setInternalState(mMockObjectStoreFileSystem, "hostNameScheme", getBaseURI());
    String result = Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, getBaseURI());
    Path modifiedInput = new Path(getBaseURI() + result);
    getFs().delete(input, false);
    Assert.assertFalse(getFs().exists(input));

    createFile(input, data);
    Assert.assertFalse(getFs().exists(input));
    Assert.assertTrue(getFs().exists(modifiedInput));
  }

  @Test
  public void listLocatedStatusTest() throws Exception {
    Assume.assumeNotNull(getFs());
    int count = 0;
    RemoteIterator<LocatedFileStatus> stats = getFs().listLocatedStatus(new Path(getBaseURI()
        + "/testFile01"));
    while (stats.hasNext()) {
      LocatedFileStatus stat = stats.next();
      Assert.assertTrue(stat.getPath().getName().startsWith("testFile01"));
      count++;
    }
    Assert.assertEquals(1, count);
  }

  @Test(expected = FileNotFoundException.class)
  public void listLocatedStatusTestNotFound1() throws Exception {
    Assume.assumeNotNull(getFs());
    int count = 0;
    RemoteIterator<LocatedFileStatus> stats = getFs().listLocatedStatus(new Path(fileName));
    while (stats.hasNext()) {
      LocatedFileStatus stat = stats.next();
      Assert.assertTrue(stat.getPath().getName().startsWith("testFile"));
      count++;
    }
    Assert.assertEquals(iterNum * 0, count);
  }

  @Test(expected = FileNotFoundException.class)
  public void listLocatedStatusTestNotFound2() throws Exception {
    Assume.assumeNotNull(getFs());
    int count = 0;
    RemoteIterator<LocatedFileStatus> stats = getFs().listLocatedStatus(new Path(fileName + "0"));
    while (stats.hasNext()) {
      LocatedFileStatus stat = stats.next();
      Assert.assertTrue(stat.getPath().getName().startsWith("testFile0"));
      count++;
    }
    Assert.assertEquals(iterNum * 0, count);
  }

  @Test
  public void openCreateTest() throws Exception {
    Assume.assumeNotNull(getFs());
    Path testFile = new Path(getBaseURI() + "/testFile");
    createFile(testFile, data);
    FSDataInputStream inputStream = getFs().open(testFile);
    String uri = (Whitebox.getInternalState(inputStream.getWrappedStream(), "uri"));
    Path path = new Path(uri);
    Assert.assertEquals(testFile.getName(), path.getName());
  }

  @Test
  public void deleteTest() throws Exception {
    Assume.assumeNotNull(getFs());

    Path testFile = new Path(getBaseURI() + "/testFile");
    createFile(testFile, data);
    Path input = new Path(getBaseURI() + "/a/b/c/m.data/_temporary/"
            + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.setInternalState(mMockObjectStoreFileSystem, "hostNameScheme", getBaseURI());
    String result = Whitebox.invokeMethod(mMockStocatorPath, "parseHadoopFOutputCommitterV1", input,
        true, getBaseURI());
    Path modifiedInput = new Path(getBaseURI() + result);
    createFile(input, data);
    Assert.assertTrue(getFs().exists(modifiedInput));
    Assert.assertTrue(getFs().exists(testFile));

    getFs().delete(testFile, false);
    Assert.assertFalse(getFs().exists(testFile));

    getFs().delete(modifiedInput, false);
    Assert.assertFalse(getFs().exists(modifiedInput));
  }

  @Test(expected = FileNotFoundException.class)
  public void listStatusTest() throws Exception {
    Assume.assumeNotNull(getFs());
    Path testFile = new Path(getBaseURI() + "/testFile");
    FileStatus[] stats = null;

    createFile(testFile, data);
    Assert.assertTrue(getFs().exists(testFile));
    stats = getFs().listStatus(testFile);
    Assert.assertEquals(1, stats.length);

    FileStatus stat = stats[0];
    Assert.assertEquals("testFile", stat.getPath().getName());
    Assert.assertFalse(stat.isDirectory());
    Assert.assertTrue(stat.isFile());
    Assert.assertEquals(data.length, stat.getLen());

    getFs().delete(testFile, false);
    stats = getFs().listStatus(testFile);
    Assert.assertEquals(0, stats.length);

    getFs().delete(testFile, false);
    stats = getFs().listStatus(testFile);
    Assert.assertEquals(0, stats.length);

  }
}
