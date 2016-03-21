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

import java.text.MessageFormat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class CollisionTest extends SwiftBaseTest {

  protected byte[] data = SwiftTestUtils.generateDataset(getBlockSize() * 2, 0, 255);
  protected byte[] smData = SwiftTestUtils.generateDataset(getBlockSize() * 1, 0, 255);

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
    if (getFs() != null) {
      getFs().delete(new Path(getBaseURI(), objectName), true);
      FileStatus[]  stats = getFs().listStatus(new Path(getBaseURI(), objectName));
      Assert.assertTrue(stats.length == 0);
      getFs().mkdirs(new Path(getBaseURI(), objectNameTmpId));
      getFs().delete(new Path(getBaseURI(), objectName1), true);
      FileStatus[]  stats1 = getFs().listStatus(new Path(getBaseURI(), objectName1));
      Assert.assertTrue(stats1.length == 0);
      getFs().mkdirs(new Path(getBaseURI(), objectNameTmpId1));
    }
  }

  @Test
  public void testDataObject() throws Exception {
    if (getFs() != null) {
      //check that object is of Spark origin
      Assert.assertTrue(true == getFs().exists(new Path(getBaseURI(), objectName)));
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
      Assert.assertTrue(stats.length == parts);
      Assert.assertTrue(true == getFs().delete(new Path(getBaseURI(), objectNameTmp), true));
      Assert.assertTrue(true == getFs().delete(new Path(getBaseURI(), objectName), true));
      FileStatus[]  stats1 = getFs().listStatus(new Path(getBaseURI(), objectName));
      Assert.assertTrue(stats1.length == 0);
    }
  }

  @Test
  public void testDataCompositeObject() throws Exception {
    if (getFs() != null) {
      //check that object is of Spark origin
      Assert.assertTrue(true == getFs().exists(new Path(getBaseURI(), objectName1)));
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
      FileStatus[]  stats = getFs().listStatus(new Path(getBaseURI(), objectName1));
      Assert.assertTrue(stats.length == parts);
      Assert.assertTrue(true == getFs().delete(new Path(getBaseURI(), objectNameTmp1), true));
      Assert.assertTrue(true == getFs().delete(new Path(getBaseURI(), objectName1), true));
      FileStatus[]  stats1 = getFs().listStatus(new Path(getBaseURI(), objectName1));
      Assert.assertTrue(stats1.length == 0);
    }
  }

}
