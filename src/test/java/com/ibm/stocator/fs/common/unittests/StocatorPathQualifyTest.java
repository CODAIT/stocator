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

package com.ibm.stocator.fs.common.unittests;

import java.net.URI;

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
import com.ibm.stocator.fs.cos.COSAPIClient;

import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;

@RunWith(PowerMockRunner.class)
public class StocatorPathQualifyTest {

  private StocatorPath mStocatorPath;
  private StocatorPath stocPath;
  private String pattern1 = "_temporary/st_ID/_temporary/attempt_ID/";
  String hostname = "cos://bucket.service";

  private COSAPIClient mCOSAPIClient;
  private String uri = "cos://bucket.service";
  private String userPath = "/user/" + System.getProperty("user.name");
  private URI filesystemURI = URI.create(uri);
  private String mBucket = "bucket";
  private Path workingDirectory = new Path("ab/ab");
  Path workingDir = null;

  @Before
  public final void before() {
    mStocatorPath = PowerMockito.mock(StocatorPath.class);
    Whitebox.setInternalState(mStocatorPath, "tempIdentifiers",
        new String[] {pattern1});
    Configuration conf = new Configuration();
    conf.setStrings("fs.stocator.temp.identifier", pattern1);
    stocPath = new StocatorPath(DEFAULT_FOUTPUTCOMMITTER_V1, conf, hostname);

    mCOSAPIClient = PowerMockito.mock(COSAPIClient.class);

    workingDir = new Path(userPath)
        .makeQualified(filesystemURI, workingDirectory);

    Whitebox.setInternalState(mCOSAPIClient, "filesystemURI", filesystemURI);
    Whitebox.setInternalState(mCOSAPIClient, "mBucket", mBucket);
    Whitebox.setInternalState(mCOSAPIClient, "workingDir", workingDir);
    Whitebox.setInternalState(mCOSAPIClient, "amazonDefaultEndpoint", "s3.amazonaws.com");

  }

  @Test
  public void isTempPathTest() throws Exception {
    Path p = new Path("/one3.txt/_temporary/0/_temporary/"
        + "attempt_201610052038_0001_m_000007_15");

    Path pQualified = qualify(p);
    String input = pQualified.toString();
    System.out.println(input);
    String expectedResult = "bucket/one3.txt";
    String result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "bucket", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);
    boolean res = stocPath.isTemporaryPathContain(new Path(input));
    Assert.assertEquals("isTemporaryPathContain() shows incorrect name",
        true, res);
    res = stocPath.isTemporaryPathTarget(new Path(input));
    Assert.assertEquals("isTemporaryPathTaget() shows incorrect name",
        true, res);

    input = "cos://bucket.service/fruit";
    expectedResult = "bucket/fruit";
    result = stocPath.getObjectNameRoot(new Path(input),
        Boolean.FALSE, "bucket", true);
    Assert.assertEquals("getObjectNameRoot() shows incorrect name",
            expectedResult, result);

  }

  public Path qualify(Path path) {
    System.out.println(workingDir);
    return path.makeQualified(filesystemURI, workingDir);
  }

}
