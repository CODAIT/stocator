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

package com.ibm.stocator.fs.commom.unittests;

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
import com.ibm.stocator.fs.common.Utils;

import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;

@RunWith(PowerMockRunner.class)
public class StocatorPathTest {

  private StocatorPath mStocatorPath;
  private StocatorPath stocPath;
  private StocatorPath stocCOSPath;
  private StocatorPath stocS3APath;
  private StocatorPath stocPathCOSEndpoint;
  private String pattern1 = "_temporary/st_ID/_temporary/attempt_ID/";
  String hostname = "swift2d://a.service/";
  String cosEndpointHostname = "cos://accesskey:secretkey@endpoint/";
  String cosHostName = "cos://a.service/";
  String s3aHostName = "s3a://a/";

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifiers",
        new String[] {pattern1});
    Configuration conf = new Configuration();
    conf.setStrings("fs.stocator.temp.identifier", pattern1);
    stocPath = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, hostname);
    stocCOSPath = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, cosHostName);
    stocS3APath = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, s3aHostName);
    stocPathCOSEndpoint = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, cosEndpointHostname);
  }

  @Test
  public void isTempPathTest() throws Exception {

    String input = "swift2d://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    String expectedResult = "a/one3.txt";
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

    input = "swift2d://a.service/fruit";
    expectedResult = "a/fruit";
    result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
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
  public void extractAccessURLTest() throws Exception {
    String input = "swift2d://a.service/a/c/d/e/f/";
    String expected = "swift2d://a.service";
    String accessURL = Utils.extractAccessURL(input, "swift2d");
    Assert.assertEquals("extractAccessURL() shows incorrect result",
        expected, accessURL);
    String hostNameScheme = accessURL + "/" + Utils.extractDataRoot(input,
        accessURL);
    expected = "swift2d://a.service/";
    Assert.assertEquals("host name scheme shows incorrect result",
        expected, hostNameScheme);
  }

  @Test
  public void isTempPathCOSEndpointURLTest() throws Exception {

    String input = "cos://accesskey:secretkey@endpoint/container/one3.txt/_temporary/0/_temporary/"
        + "attempt_201710121127_0001_m_000007_15";
    String expectedResult = "container/one3.txt";
    String result = stocPathCOSEndpoint.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "container", false);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    boolean res = stocPathCOSEndpoint.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocPathCOSEndpoint.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);

    input = "cos://accesskey:secretkey@endpoint/container/fruit";
    expectedResult = "container/fruit";
    result = stocPathCOSEndpoint.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "container", false);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void parseHadoopDefaultCOSEndpointPathTest() throws Exception {

    String hostname = "cos://accesskey:secretkey@endpoint/container";

    String input = "cos://accesskey:secretkey@endpoint/container/aa/bb/cc/one3.txt/_temporary/0/"
        + "_temporary/attempt_201610052038_0001_m_000007_15/part-00007";
    String expectedResult = "/aa/bb/cc/one3.txt/part-00007";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://accesskey:secretkey@endpoint/container/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/a/part-00007";
    expectedResult = "/one3.txt/a/part-00007";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://accesskey:secretkey@endpoint/container/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/";
    expectedResult = "/one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://accesskey:secretkey@endpoint/container/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    expectedResult = "/one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://accesskey:secretkey@endpoint/container/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    expectedResult = "/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void extractAccessCOSEndpointURLTest() throws Exception {
    String input = "cos://accesskey:secretkey@endpoint/container/a/c/d/e/f/";
    String expected = "cos://accesskey:secretkey@endpoint";
    String accessURL = Utils.extractAccessURL(input, "cos");
    Assert.assertEquals("extractAccessURL() shows incorrect result",
        expected, accessURL);
    String hostNameScheme = accessURL + "/" + Utils.extractDataRoot(input,
        accessURL);
    expected = "cos://accesskey:secretkey@endpoint/container";
    Assert.assertEquals("host name scheme shows incorrect result",
        expected, hostNameScheme);
  }

  @Test
  public void isCOSTempPathTest() throws Exception {

    String input = "cos://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    String expectedResult = "a/one3.txt";
    String result = stocCOSPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
   // Assert.assertEquals("getObjectNameRoot() shows incorrect name",
   //         expectedResult, result);
    boolean res = stocCOSPath.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocCOSPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);

    input = "cos://a.service/fruit";
    expectedResult = "a/fruit";
    result = stocCOSPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void parseCOSHadoopDefaultPathTest() throws Exception {

    String hostname = "cos://a.service/";

    String input = "cos://a.service/aa/bb/cc/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/part-00007";
    String expectedResult = "aa/bb/cc/one3.txt/part-00007";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/a/part-00007";
    expectedResult = "one3.txt/a/part-00007";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://a.service/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "cos://a.service/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void extractCOSAccessURLTest() throws Exception {
    String input = "cos://a.service/a/c/d/e/f/";
    String expected = "cos://a.service";
    String accessURL = Utils.extractAccessURL(input, "cos");
    Assert.assertEquals("extractAccessURL() shows incorrect result",
        expected, accessURL);
    String hostNameScheme = accessURL + "/" + Utils.extractDataRoot(input,
        accessURL);
    expected = "cos://a.service/";
    Assert.assertEquals("host name scheme shows incorrect result",
        expected, hostNameScheme);
  }

  @Test
  public void isS3ATempPathTest() throws Exception {

    String input = "s3a://a/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    String expectedResult = "a/one3.txt";
    String result = stocS3APath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    boolean res = stocS3APath.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocS3APath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);

    input = "s3a://a/fruit";
    expectedResult = "a/fruit";
    result = stocS3APath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void parseS3AHadoopDefaultPathTest() throws Exception {

    String hostname = "s3a://a/";

    String input = "s3a://a/aa/bb/cc/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/part-00007";
    String expectedResult = "aa/bb/cc/one3.txt/part-00007";
    String result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "s3a://a/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/a/part-00007";
    expectedResult = "one3.txt/a/part-00007";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "s3a://a/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15/";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "s3a://a/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

    input = "s3a://a/one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    expectedResult = "one3.txt/_temporary/0/_temporary/"
        + "attampt_201610052038_0001_m_000007_15";
    result = Whitebox.invokeMethod(mStocatorPath, "extractNameFromTempPath",
        new Path(input), false, hostname, false);
    Assert.assertEquals("extractObectNameFromTempPath() shows incorrect name",
            expectedResult, result);

  }

  @Test
  public void extractS3AAccessURLTest() throws Exception {
    String input = "s3a://a/a/c/d/e/f/";
    String expected = "s3a://a";
    String accessURL = Utils.extractAccessURL(input, "s3a");
    Assert.assertEquals("extractAccessURL() shows incorrect result",
        expected, accessURL);
    String hostNameScheme = accessURL + "/" + Utils.extractDataRoot(input,
        accessURL);
    expected = "s3a://a/";
    Assert.assertEquals("host name scheme shows incorrect result",
        expected, hostNameScheme);
  }

  public void partitionsPathTest() throws Exception {

    String input = "swift2d://a.service/aa/abc.parquet/"
        + "_temporary/0/_temporary/attempt_20171115113432_0017_m_000076_0/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet";
    String expectedResult = "a/aa/abc.parquet/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet"
        + "-attempt_20171115113432_0017_m_000076_0";

    String result = stocPath.getObjectNameRoot(new Path(input), true, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    input = "swift2d://a.service/aa/abc.parquet/"
        + "_temporary/0/_temporary/attempt_20171115113432_0017_m_000076_0/"
        + "YEAR=2003/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet";
    expectedResult = "a/aa/abc.parquet/"
        + "YEAR=2003/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet"
        + "-attempt_20171115113432_0017_m_000076_0";

    result = stocPath.getObjectNameRoot(new Path(input), true, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

    input = "swift2d://a.service/aa/abc.parquet/"
        + "_temporary/0/_temporary/attempt_20171115113432_0017_m_000076_0/"
        + "D_DATE=2003-01-10 00%3A00%3A00/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet";
    expectedResult = "a/aa/abc.parquet/"
        + "D_DATE=2003-01-10 00%3A00%3A00/"
        + "part-00076-335c9928-ccbb-4830-b7e3-0348a7d7d8f8.snappy.parquet"
        + "-attempt_20171115113432_0017_m_000076_0";

    result = stocPath.getObjectNameRoot(new Path(input), true, "a", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

  }
}
