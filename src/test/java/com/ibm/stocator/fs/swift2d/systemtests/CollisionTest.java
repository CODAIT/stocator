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

import java.text.MessageFormat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.ibm.stocator.fs.common.FileSystemTestUtils;

public class CollisionTest extends SwiftBaseTest {

  protected byte[] data = FileSystemTestUtils.generateDataset(getBlockSize() * 2, 0, 255);
  protected byte[] smData = FileSystemTestUtils.generateDataset(getBlockSize(), 0, 255);

  private String objectName = "/data7.txt";
  private String objectNameTmp = objectName + "/_temporary";
  private String objectNameTmpId = objectNameTmp + "/0";
  private String sparkSuccessFormat = objectName + "/_SUCCESS";

  private String objectName1 = "/a1/b1/d1/data7.txt";
  private String objectNameTmp1 = objectName1 + "/_temporary";
  private String objectNameTmpId1 = objectNameTmp1 + "/0";
  private String sparkSuccessFormat1 = objectName1 + "/_SUCCESS";

  private int parts = 11;

  // {1}, {3} = two digits, starting with 00
  private String sparkPutFormat = "{0}/_temporary/0/_temporary/"
      + "attempt_201612062056_0000_m_0000{1}_{2}/part-000{3}";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assume.assumeNotNull(getFs());
    getFs().delete(new Path(getBaseURI(), objectName), true);
    boolean res = getFs().exists(new Path(getBaseURI(), objectName));
    Assert.assertTrue(false == res);
    getFs().mkdirs(new Path(getBaseURI(), objectNameTmpId));
    getFs().delete(new Path(getBaseURI(), objectName1), true);
    res = getFs().exists(new Path(getBaseURI(), objectName1));
    Assert.assertTrue(res == false);
    getFs().mkdirs(new Path(getBaseURI(), objectNameTmpId1));
  }

  @Test
  public void testDataObject() throws Exception {
    //check that object is of Spark origin
    Assert.assertTrue(getFs().exists(new Path(getBaseURI(), objectName)));
    Object[] params;
    for (int i = 0;i < parts; i++) {
      String id = String.format("%0" + 2 + "d", i);
      params = new Object[]{objectName, id, String.valueOf(i), id};
      Path path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, smData);
      // create failed tasks
      params = new Object[]{objectName, id, String.valueOf(i + 1), id};
      path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, smData);
      // create failed tasks
      params = new Object[]{objectName, id, String.valueOf(i + 2), id};
      path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, data);
    }
    // print created objects
    createEmptyFile(new Path(getBaseURI(), sparkSuccessFormat));
    FileStatus[]  stats = getFs().listStatus(new Path(getBaseURI(), objectName));
    Assert.assertEquals(parts, stats.length);
    Assert.assertTrue(getFs().delete(new Path(getBaseURI(), objectNameTmp), true));
    Assert.assertTrue(getFs().delete(new Path(getBaseURI(), objectName), true));
    boolean res = getFs().exists(new Path(getBaseURI(), objectName));
    Assert.assertTrue(res == false);
  }

  @Test
  public void testDataCompositeObject() throws Exception {
    //check that object is of Spark origin
    Assert.assertTrue(getFs().exists(new Path(getBaseURI(), objectName1)));
    Object[] params;
    for (int i = 0;i < parts; i++) {
      String id = String.format("%0" + 2 + "d", i);
      params = new Object[]{objectName1, id, String.valueOf(i), id};
      Path path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, smData);
      // create failed tasks
      params = new Object[]{objectName1, id, String.valueOf(i + 1), id};
      path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, smData);
      // create failed tasks
      params = new Object[]{objectName1, id, String.valueOf(i + 2), id};
      path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
      createFile(path, data);
    }
    // print created objects
    createEmptyFile(new Path(getBaseURI(), sparkSuccessFormat1));
    FileStatus[] stats = getFs().listStatus(new Path(getBaseURI(), objectName1));
    Assert.assertEquals(parts, stats.length);
    Assert.assertTrue(getFs().delete(new Path(getBaseURI(), objectNameTmp1), true));
    Assert.assertTrue(getFs().delete(new Path(getBaseURI(), objectName1), true));
    boolean res = getFs().exists(new Path(getBaseURI(), objectName1));
    Assert.assertTrue(res == false);
  }

}
