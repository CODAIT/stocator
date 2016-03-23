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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import com.ibm.stocator.fs.swift.SwiftAPIClient;

public class SwiftAPIClientTest {

  private SwiftAPIClient mSwiftAPIClient;

  @Before
  public final void before() {
    mSwiftAPIClient = PowerMockito.mock(SwiftAPIClient.class);
  }

  @Test
  public void extractUnifiedObjectNameTest() throws Exception {
    String objectUnified = "a/b/c/gil.data";

    String input = objectUnified;
    String result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals(input, result);

    input = objectUnified + "/_SUCCESS";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals(objectUnified, result);

    input = objectUnified + "/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv-"
        + "attempt_201603171328_0000_m_000000_1";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals(objectUnified, result);

    input = "a/b/c/gil.data/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv-"
        + "attempt_20160317132a_wrong_0000_m_000000_1";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals(input, result);
  }
}
