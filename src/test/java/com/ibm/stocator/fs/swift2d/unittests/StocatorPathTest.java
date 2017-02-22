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

import java.util.ArrayList;
import java.util.List;

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
import com.ibm.stocator.fs.common.Tuple;

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
  private String pattern7 = "_SCRATCH0.ID/_temporary/st_ID/_temporary/attempt_ID/";
  private String pattern8 = "_SCRATCH0.ID";
  private String pattern9 = "TEMP_ID";
  private String pattern10 = ".COMMITTING__TEMP_ID";
  private String pattern11 = ".distcp.tmp.attempt_ID";
  String hostname = "swift2d://a.service/";

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifiers",
        new String[] {pattern6, pattern7, pattern8, pattern9,
            pattern10, pattern2, pattern3, pattern4, pattern5, pattern1, pattern11});
    Configuration conf = new Configuration();
    conf.setStrings("fs.stocator.temp.identifier", pattern6, pattern7, pattern8, pattern9,
        pattern10, pattern2, pattern3, pattern4, pattern5, pattern1, pattern11);
    stocPath = new StocatorPath(CUSTOM_FOUTPUTCOMMITTER, conf, hostname);

  }

  @Test
  public void parseTestV1() throws Exception {
    String hostname = "swift2d://a.service/";
    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1/"
        + "_tmp.-ext-10002/color=Yellow/000000_0";
    String expectedResult = "fruit_hive_dyn/color=Yellow/000000_0";

    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input =  "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_11-07-53_413_8347422774094227881-1/"
        + "_task_tmp.-ext-10002/color=Yellow/_tmp.000000_0";
    expectedResult = "fruit_hive_dyn/color=Yellow/_tmp.000000_0";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_23d/"
        + ".hive-staging_hive_2017-01-31_15-58-51_954_1542283154702952521-1/_tmp.-ext-10002";
    expectedResult = "fruit_23d";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_23d/"
        + ".hive-staging_hive_2017-02-01_08-56-17_725_2916730392594052694-1/-ext-10002";
    expectedResult = "fruit_23d";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "_DYN0.600389881457611886943120206775524854029/color=Green/"
        + "_temporary/1/_temporary/attempt_1484176830822_0004_r_000003_0";
    expectedResult = "fruit_hive_dyn/color=Green/attempt_1484176830822_0004_r_000003_0";

    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);

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

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "TEMP_1486552852171_590903925_201702080320928";
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    input = "swift2d://a.service/fruit_hive_dyn/"
        + ".COMMITTING__TEMP_1486552852171_590903925_201702080320928";
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + ".COMMITTING__TEMP_1486555911917_-764810680_201702080411141";
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);

    input = "swift2d://a.service/data3/.distcp.tmp.attempt_local2036034928_0001_m_000000_0";
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
  }

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
  public void parseHadoopDefaultPathTest() throws Exception {

    String hostname = "swift2d://a.service/";

    String input = "swift2d://a.service/aa/bb/cc/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/part-00007";
    String expectedResult = "aa/bb/cc/one3.txt/part-00007";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/a/part-00007";
    expectedResult = "one3.txt/a/part-00007";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
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
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2017-02-07_13-34-29_470_1053856006407136479-1/"
        + "_task_tmp.-ext-10002/color=Red/_tmp.000000_0";
    expectedResult = "fruit_hive_dyn/color=Red/"
        + "_tmp.000000_0-2017-02-07_13-34-29_470_1053856006407136479-1";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), true, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "_SCRATCH0.2522929312513381824643989192318958144030/_temporary/1/"
        + "_temporary/attempt_1486475505018_0008_m_000000_0/part-m-00000";
    expectedResult = "fruit_hive_dyn/part-m-00000-attempt_1486475505018_0008_m_000000_0";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), true, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "_SCRATCH0.2522929312513381824643989192318958144030/_temporary/1/"
        + "_temporary/attempt_1486475505018_0008_m_000000_0/";
    expectedResult = "fruit_hive_dyn";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), true, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "_SCRATCH0.2522929312513381824643989192318958144030/";
    expectedResult = "fruit_hive_dyn";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), true, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "_SCRATCH0.3655423817421201183321170691664364082/_SUCCESS";
    expectedResult = "fruit_hive_dyn/_SUCCESS";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + "TEMP_1486546384743_865956912_201702080143406";
    expectedResult = "fruit_hive_dyn";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void partitionTest() throws Exception {

    List<Tuple<String, String>> partitions = new ArrayList<>();
    partitions.add(new Tuple<String, String>("a", "b"));
    partitions.add(new Tuple<String, String>("c", "d"));

    String input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2016-12-21_08-46-44_430_2111117233601747099-1";
    String partList = "";
    for (Tuple<String, String> part : partitions) {
      partList = partList + part.x + "=" + part.y + "/";
    }
    input = input + "/" + partList;
    List<Tuple<String, String>> result = stocPath.getAllPartitions(input);
    int ind = 0;
    for (Tuple<String, String> t : result) {
      Assert.assertEquals("getAllPartitions failed ",t, partitions.get(ind));
      ind++;
    }
    boolean isPartitionPath = stocPath.isPartitionTarget(new Path(input));
    Assert.assertEquals("isPartitionTarget", isPartitionPath, true);

    input = "swift2d://a.service/fruit_hive_dyn/"
        + ".hive-staging_hive_2017-02-14_18-01-52_833_2074202740199836878-1/"
        + "_task_tmp.-ext-10002/color=Green/_tmp.000000_0";
    String res = stocPath.getGlobalPrefixName(new Path(input), hostname, true);
    String expected = input = "swift2d://a.service/fruit_hive_dyn";
    Assert.assertEquals("getGlobalPrefixName", expected, res);
  }

}
