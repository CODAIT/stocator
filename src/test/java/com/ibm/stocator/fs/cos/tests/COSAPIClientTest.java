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

package com.ibm.stocator.fs.cos.tests;

import java.net.URI;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.fs.Path;

import com.ibm.stocator.fs.cos.COSAPIClient;

@RunWith(PowerMockRunner.class)
public class COSAPIClientTest {

  private COSAPIClient mCOSAPIClient;
  private String uri = "cos://bucket.service";
  private String userPath = "/user/" + System.getProperty("user.name");
  private URI filesystemURI = URI.create(uri);
  private String mBucket = "bucket";
  private Path workingDirectory = new Path("ab/ab");

  @Before
  public final void before() {
    mCOSAPIClient = PowerMockito.mock(COSAPIClient.class);

    Path workingDir = new Path(userPath)
        .makeQualified(filesystemURI, workingDirectory);

    Whitebox.setInternalState(mCOSAPIClient, "filesystemURI", filesystemURI);
    Whitebox.setInternalState(mCOSAPIClient, "mBucket", mBucket);
    Whitebox.setInternalState(mCOSAPIClient, "workingDir", workingDir);
    Whitebox.setInternalState(mCOSAPIClient, "amazonDefaultEndpoint", "s3.amazonaws.com");
  }

  @Test
  public void innerQualifyTest() throws Exception {
    Path f1 = new Path("a/b/c/gil.data");

    Path res1 = Whitebox.invokeMethod(mCOSAPIClient, "innerQualify", f1);
    Assert.assertEquals("Doesn't match", new Path(new Path(uri,userPath), f1).toString(),
        res1.toString());
    System.out.println(res1);
    f1 = new Path(uri, "/a/b/c/gil.data");
    res1 = Whitebox.invokeMethod(mCOSAPIClient, "innerQualify", f1);
    Assert.assertEquals("Doesn't match", f1.toString(),
        res1.toString());
  }

  @Test
  public void pathToKeyTest() throws Exception {
    Path f1 = new Path("a/b/c/gil.data");
    Path res1 = Whitebox.invokeMethod(mCOSAPIClient, "innerQualify", f1);
    String res2 = Whitebox.invokeMethod(mCOSAPIClient, "pathToKey", res1);

    f1 = new Path(uri, "/a/b/c/gil.data");
    String res3 = Whitebox.invokeMethod(mCOSAPIClient, "pathToKey", f1);

    f1 = new Path(uri, "/gil.data");
    res3 = Whitebox.invokeMethod(mCOSAPIClient, "pathToKey", f1);

    f1 = new Path("cos://bucket/data.txt");
    res3 = Whitebox.invokeMethod(mCOSAPIClient, "pathToKey", f1);

  }

}
