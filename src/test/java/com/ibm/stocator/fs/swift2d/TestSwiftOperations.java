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
  }

  @Test
  public void testDataObject() throws Exception {
    if (getFs() != null) {
      String objectName = "data7.txt";
      Object[] params;
      for (int i = 0;i < 11; i++) {
        String id = String.format("%0" + 2 + "d", i);
        params = new Object[]{objectName, id, String.valueOf(i), id};
        Path path = new Path(getBaseURI(), MessageFormat.format(sparkPutFormat, params));
        createFile(path, data);
      }
      // print created objects
      createEmptyFile(new Path(getBaseURI(),
          MessageFormat.format(sparkSuccessFormat, new Object[]{objectName})));
      FileStatus[]  stats = getFs().listStatus(new Path(getBaseURI() + "/" + objectName));
      Assert.assertTrue(11 == stats.length);
      for (int i = 0;i < 11; i++) {
        String id = String.format("%0" + 2 + "d", i);
        params = new Object[]{objectName, id, String.valueOf(i), id};
        Path path = new Path(getBaseURI(), MessageFormat.format(swiftDataFormat, params));
        System.out.print(".");
        byte[] res = SwiftTestUtils.readDataset(getFs(),
            path, data.length);
        Assert.assertArrayEquals(data, res);
      }
      for (int i = 0;i < 11; i++) {
        String id = String.format("%0" + 2 + "d", i);
        params = new Object[]{objectName, id, String.valueOf(i), id};
        Path path = new Path(getBaseURI(), MessageFormat.format(swiftDataFormat, params));
        System.out.print(".");
        getFs().delete(path, false);
      }
      stats = getFs().listStatus(new Path(getBaseURI() + "/" + objectName));
      Assert.assertTrue(0 == stats.length);
    }
  }
}
