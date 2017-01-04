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
import static com.ibm.stocator.fs.common.Constants.HIVE_STAGING_DEFAULT;

@RunWith(PowerMockRunner.class)
public class StocatorPathTest {

  private StocatorPath mStocatorPath;

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempFileOriginator", HIVE_OUTPUT_V1);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifier", HIVE_STAGING_DEFAULT + "_hive_");
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

    input =  "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_11-07-53_413_8347422774094227881-1/"
        + "_task_tmp.-ext-10002/color=Yellow/_tmp.000000_0";
    expectedResult = "fruit_hive_dyn/color=Yellow/000000_0";
    result = Whitebox.invokeMethod(mStocatorPath, "parseHiveV1", new Path(input), hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);
  }

  @Test
  public void isTempPathTest() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1";
    String expectedResult = "a/fruit_hive_dyn";
    StocatorPath stocPath = new StocatorPath(HIVE_OUTPUT_V1, null);
    String result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", hostname, true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    boolean res = stocPath.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);

    input = "swift2d://a.service/fruit_hive_dyn";
    expectedResult = "a/fruit_hive_dyn";
    result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", hostname, true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
  }

  @Test
  public void isTempPathTest2() throws Exception {
    String hostname = "swift2d://a.service/";
    String input1 = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_15-53-10_468_3659726004869556488-1";
    String expectedResult1 = "a/fruit_hive_dyn";
    String input2 = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_15-53-10_468_3659726004869556488-1/-ext-10001";
    String input3 = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_15-53-10_468_3659726004869556488-1/_tmp.-ext-10002";
    String input4 = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_15-53-10_468_3659726004869556488-1/"
        + "_tmp.-ext-10002/color=Red";
    String expectedResult4 = "a/fruit_hive_dyn/color=Red";

    StocatorPath stocPath = new StocatorPath(HIVE_OUTPUT_V1, null);
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        true, stocPath.isTemporaryPathTarget(new Path(input1)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        false, stocPath.isTemporaryPathTarget(new Path(input2)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        false, stocPath.isTemporaryPathTarget(new Path(input3)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        true, stocPath.isTemporaryPathTarget(new Path(input4)));

    String result = stocPath.getObjectNameRoot(new Path(input1),
        Boolean.FALSE, "a", hostname, true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult1, result);
    result = stocPath.getObjectNameRoot(new Path(input2),
        Boolean.FALSE, "a", hostname, true);
    result = stocPath.getObjectNameRoot(new Path(input4),
        Boolean.FALSE, "a", hostname, true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult4, result);
  }

  @Test
  public void generateCopyObject() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-22_12-18-37_120_6548958596373206731-1/_tmp.-ext-10002/"
        + "color=Green/000000_0";
    String expectedResult = "swift2d://a.service/fruit_hive_dyn/"
        + "color=Green/000000_0";
    StocatorPath stocPath = new StocatorPath(HIVE_OUTPUT_V1, null);
    String result = stocPath.getActualPath(new Path(input),
        Boolean.FALSE, "a", hostname);
    Assert.assertEquals("getActualPath() shows incorrect name",
            expectedResult, result);
  }

}
