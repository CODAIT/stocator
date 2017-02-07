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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ibm.stocator.fs.common.StocatorPath;

import static com.ibm.stocator.fs.common.Constants.CUSTOM_FOUTPUTCOMMITTER;

@RunWith(PowerMockRunner.class)
public class StocatorPathTest {

  private StocatorPath mStocatorPath;
  private StocatorPath stocPath;
  private String pattern1 = "_temporary/st_ID/_temporary/attempt_ID/";
  private String pattern2 = ".hive-staging_hive_ID/_tmp.-ext-ID1";
  private String pattern3 = ".hive-staging_hive_ID/_task_tmp.-ext-ID2";
  private String pattern4 = ".hive-staging_hive_ID/-ext-ID1";
  private String pattern5 = "_DYN0.ID";
  private String pattern6 = "_DYN0.ID/_ADD_/_temporary/st_ID/_temporary/";
  String hostname = "swift2d://a.service/";

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifiers",
        new String[] {pattern6, pattern2, pattern3, pattern4, pattern5, pattern1});
    Configuration conf = new Configuration();
    conf.setStrings("fs.stocator.temp.identifier", pattern6, pattern2,
        pattern3, pattern4, pattern5, pattern1);
    stocPath = new StocatorPath(CUSTOM_FOUTPUTCOMMITTER, conf, hostname);

  }

  @Test
  public void parseHiveV1Test() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/"
        + "_tmp.-ext-10002/color=Yellow/000000_0";
    String expectedResult = "fruit_hive_dyn/color=Yellow/000000_0";

    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input =  "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_11-07-53_413_8347422774094227881-1/"
        + "_task_tmp.-ext-10002/color=Yellow/_tmp.000000_0";
    expectedResult = "fruit_hive_dyn/color=Yellow/_tmp.000000_0";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_23d/"
        + ".hive-staging_hive_2017-01-31_15-58-51_954_1542283154702952521-1/_tmp.-ext-10002";
    expectedResult = "fruit_23d";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_23d/"
        + ".hive-staging_hive_2017-02-01_08-56-17_725_2916730392594052694-1/-ext-10002";
    expectedResult = "fruit_23d";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void isTempPathTest() throws Exception {

    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1";
    String expectedResult = "a/fruit_hive_dyn";
    String result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
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
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
  }

  /*
  @Test
  public void isTempPathTest2() throws Exception {
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

    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        true, stocPath.isTemporaryPathTarget(new Path(input1)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        false, stocPath.isTemporaryPathTarget(new Path(input2)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        false, stocPath.isTemporaryPathTarget(new Path(input3)));
    Assert.assertEquals("isTemporaryPathTarget() shows incorrect name",
        true, stocPath.isTemporaryPathTarget(new Path(input4)));

    String result = stocPath.getObjectNameRoot(new Path(input1),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult1, result);
    result = stocPath.getObjectNameRoot(new Path(input2),
        Boolean.FALSE, "a", true);
    result = stocPath.getObjectNameRoot(new Path(input4),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult4, result);
  }
   */
  @Test
  public void generateCopyObject() throws Exception {
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-22_12-18-37_120_6548958596373206731-1/_tmp.-ext-10002/"
        + "color=Green/000000_0";
    String expectedResult = "swift2d://a.service/fruit_hive_dyn/"
        + "color=Green/000000_0";
    String result = stocPath.getActualPath(new Path(input),
        Boolean.FALSE, "a");
    Assert.assertEquals("getActualPath() shows incorrect name",
            expectedResult, result);
  }

  @Test
  public void parseHcatalogV1Test() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + "_DYN0.600389881457611886943120206775524854029/color=Green/"
        + "_temporary/1/_temporary/attempt_1484176830822_0004_r_000003_0";
    String expectedResult = "fruit_hive_dyn/color=Green/attempt_1484176830822_0004_r_000003_0";

    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);

    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);
  }

  @Test
  public void parseHadoopDefaultPathTest() throws Exception {

    String hostname = "swift2d://a.service/";

    String input = "swift2d://a.service/aa/bb/cc/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/part-00007";
    String expectedResult = "aa/bb/cc/one3.txt/part-00007";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/a/part-00007";
    expectedResult = "one3.txt/a/part-00007";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void parseHivePathTest() throws Exception {
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-22_12-18-37_120_6548958596373206731-1/_tmp.-ext-10002/"
        + "color=Green/000000_0";
    String expectedResult = "fruit_hive_dyn/color=Green/000000_0";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2017-02-07_13-34-29_470_1053856006407136479-1/"
        + "_task_tmp.-ext-10002/color=Red/_tmp.000000_0";
    expectedResult = "fruit_hive_dyn/color=Red/"
        + "_tmp.000000_0-2017-02-07_13-34-29_470_1053856006407136479-1";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), true, hostname);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

}
