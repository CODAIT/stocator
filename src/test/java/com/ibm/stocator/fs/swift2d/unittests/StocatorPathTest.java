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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.fs.Path;

import com.ibm.stocator.fs.common.StocatorPath;

import static com.ibm.stocator.fs.common.Constants.HIVE_OUTPUT_V1;
import static com.ibm.stocator.fs.common.Constants.HIVE_STAGING_TEMPORARY;

@RunWith(PowerMockRunner.class)
public class StocatorPathTest {

  private StocatorPath mStocatorPath;

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempFileOriginator", HIVE_OUTPUT_V1);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifier", HIVE_STAGING_TEMPORARY);
  }

  @Test
  public void parseHiveV1Test() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/"
        + "_tmp.-ext-10002/color=Yellow/000000_0";
    String expectedResult = "fruit_hive_dyn/color=Yellow/000000_0";

    String result = Whitebox.invokeMethod(mStocatorPath, "parseHiveV1", new Path(input), hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);
  }

  @Test
  public void isTempPathTest() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1";
    String expectedResult = "a/fruit_hive_dyn";
    StocatorPath stocPath = new StocatorPath(HIVE_OUTPUT_V1);
    String result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", hostname);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    boolean res = stocPath.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocPath.isTemporaryPathTaget(new Path(input).getParent());
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);
  }
}
