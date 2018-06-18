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

import java.util.HashMap;
import java.util.Locale;

import org.javaswift.joss.model.StoredObject;

import org.javaswift.joss.client.mock.ContainerMock;
import org.javaswift.joss.client.mock.StoredObjectMock;
import org.javaswift.joss.client.mock.AccountMock;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import com.ibm.stocator.fs.swift.SwiftAPIClient;
import com.ibm.stocator.fs.swift.auth.JossAccount;

import static com.ibm.stocator.fs.common.Constants.HADOOP_SUCCESS;
import static com.ibm.stocator.fs.common.Utils.lastModifiedAsLong;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StoredObject.class)
public class SwiftAPIClientTest {

  private SwiftAPIClient mSwiftAPIClient;
  private StoredObjectMock mStoredObject;
  private AccountMock mAccount;
  private JossAccount mJossAccount;
  private ContainerMock mContainer;
  private HashMap mHashMap;
  private String mContainerName;

  @Before
  public final void before() {
    mSwiftAPIClient = PowerMockito.mock(SwiftAPIClient.class);
    mAccount = new AccountMock();
    mJossAccount = PowerMockito.mock(JossAccount.class);
    mContainerName = "aa-bb-cc";
    mContainer = (ContainerMock)new ContainerMock(mAccount, mContainerName).create();
    mHashMap = PowerMockito.spy(new HashMap<String, Boolean>());

    Whitebox.setInternalState(mSwiftAPIClient, "cachedSparkJobsStatus", mHashMap);
    Whitebox.setInternalState(mSwiftAPIClient, "cachedSparkOriginated", mHashMap);
    Whitebox.setInternalState(mSwiftAPIClient, "container", mContainerName);
    Whitebox.setInternalState(mJossAccount, "mAccount", mAccount);
    PowerMockito.when(mJossAccount.getAccount()).thenReturn(mAccount);
    Whitebox.setInternalState(mSwiftAPIClient, "mJossAccount", mJossAccount);
  }

  @Test
  public void extractUnifiedObjectNameTest() throws Exception {
    String objectUnified = "a/b/c/gil.data";

    String input = objectUnified;
    String result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name",
            input, result);

    input = objectUnified + "/_SUCCESS";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with _SUCCESS",
            objectUnified, result);

    input = objectUnified + "/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c"
        + "-attempt_201603171328_0000_m_000000_1"
        + ".csv";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with attempt",
            objectUnified, result);

    input = "a/b/c/gil.data/"
        + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c"
        + "-attempt_20160317132a_wrong_0000_m_000000_1"
        + ".csv";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "extractUnifiedObjectName", input);
    Assert.assertEquals("extractUnifiedObjectName() shows incorrect name with wrong taskAttemptID",
            input, result);
  }

  @Test
  public void nameWithoutTaskIDTest() throws Exception {
    String objectName = "a/b/c/gil.data/"
            + "part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c";

    String input = objectName + "-attempt_201603171328_0000_m_000000_1.csv";
    String result = Whitebox.invokeMethod(mSwiftAPIClient, "nameWithoutTaskID", input);
    Assert.assertEquals("nameWithoutTaskID() shows incorrect name",
            objectName + ".csv", result);

    input = objectName + "-attempt_20160317132a_wrong_0000_m_000000_1.csv";
    result = Whitebox.invokeMethod(mSwiftAPIClient, "nameWithoutTaskID", input);
    Assert.assertEquals("nameWithoutTaskID() shows incorrect name with wrong taskAttemptID",
            input, result);
  }

  @Test
  public void getMergedPathTest() throws Exception {
    String hostName = "swift2d://aa-bb-cc.lvm/";
    String hostName2 = "swift2d://aa-bb-cc-dd.lvm/";
    String pathName = "data7-1-23-a.txt";
    String objectName = "data7-1-23-a.txt/part-00002-attempt_201612062056_0000_m_000002_2";

    //test when the objectName passed in is the same as the pathName
    String result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), pathName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name is the same as the path name",
            hostName + pathName, result);

    //test when the objectName starts with the pathName
    result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name starts with the path name",
            hostName + objectName, result);

    //test when the objectName is not the same, nor does start with, the pathName
    result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(hostName + pathName), "ABC");
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the object name is not the same, nor does start with, the path name",
            hostName + pathName, result);

    //test when the hostName is not part of the Path
    result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when the host name is not part of the Path",
            hostName + objectName, result);

    //test when a different hostName is part of the Path
    result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(hostName2 + pathName), objectName);
    Assert.assertEquals("getMergedPath() shows incorrect merged name "
            + "when a different host name is part of the Path",
            hostName + objectName, result);
  }

  @Test
  public void getLastModifiedTest() throws Exception {
    String stringTime = "Fri, 06 May 2016 03:44:47 GMT";
    long longTime = 1462506287000L;

    // test to see if getLastModified parses date with default locale
    long result = lastModifiedAsLong(stringTime);
    Assert.assertEquals("getLastModified() shows incorrect time",
            longTime, result);

    // test to see if getLastModified parses date when default locale is different than US
    Locale originalLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.ITALIAN);
      result = lastModifiedAsLong(stringTime);
      Assert.assertEquals("getLastModified() shows incorrect time",
              longTime, result);
    } finally {
      Locale.setDefault(originalLocale);
    }
  }

  @Test
  public void isJobSuccessfulTest() throws Exception {
    String objectName = "data7-1-23-a.txt/part-00002-attempt_201612062056_0000_m_000002_2";

    //test to see if _SUCCESS object does not exist
    mStoredObject = new StoredObjectMock(mContainer, objectName + "/" + HADOOP_SUCCESS);
    Assert.assertEquals("isJobSuccessful() shows _SUCCESS object already exists",
            false, mStoredObject.exists());

    //test to see if _SUCCESS object exists
    mStoredObject.uploadObject(new byte[]{});
    Assert.assertEquals("isJobSuccessful() shows _SUCCESS object does not exist",
            true, mStoredObject.exists());

    //test when _HADOOP_SUCCESS exists
    boolean result = Whitebox.invokeMethod(mSwiftAPIClient, "isJobSuccessful", objectName);
    Assert.assertEquals("isJobSuccessful() failed even when HADOOP_SUCCESS exists",
            true, result);

    //test to see if job status is cached properly
    HashMap jobStatus = Whitebox.getInternalState(mSwiftAPIClient, "cachedSparkJobsStatus");
    Assert.assertEquals("isJobSuccessful() shows job status is not cached correctly",
            true, jobStatus.containsKey(objectName));

    //test to see if it obtains status from the cache
    result = Whitebox.invokeMethod(mSwiftAPIClient, "isJobSuccessful", objectName);
    Assert.assertEquals("isJobSuccessful() shows job status is not obtained from cache",
            true, result);

    //test when the container name is part of the object name
    result = Whitebox.invokeMethod(mSwiftAPIClient, "isJobSuccessful",
            mContainerName + "/" + objectName);
    Assert.assertEquals("isJobSuccessful() failed when the container name"
            + "is part of the object name",
            false, result);
  }

  @Test
  public void setCorrectSizeTest() throws Exception {
    String objectName = "data7-1-23-a.txt/part-00002-attempt_201612062056_0000_m_000002_2";
    mStoredObject = new StoredObjectMock(mContainer, objectName);
    mStoredObject.uploadObject(new byte[]{1,2,3});

    //test to see if correct size is returned
    Whitebox.invokeMethod(mSwiftAPIClient, "setCorrectSize",
            mStoredObject, mContainer);
    Assert.assertEquals("setCorrectSize() shows incorrect size returned",
            3, mStoredObject.getContentLength());
  }

  @Test
  public void getFileStatusTest() throws Exception {
    String objectName = "data7-1-23-a.txt/part-00002-attempt_201612062056_0000_m_000002_2";
    String hostName = "swift2d://aa-bb-cc.lvm/";
    String pathName = "data7-1-23-a.txt";
    mStoredObject = new StoredObjectMock(mContainer, objectName);
    mStoredObject.uploadObject(new byte[]{1, 2, 3});

    //test to see if correct length is returned
    FileStatus fs = Whitebox.invokeMethod(mSwiftAPIClient, "createFileStatus",
            mStoredObject, mContainer, hostName, new Path(pathName));
    Assert.assertEquals("getFileStatus() shows incorrect length",
            3, fs.getLen());

    //test to see if correct path is returned
    String result = Whitebox.invokeMethod(mSwiftAPIClient, "getMergedPath",
            hostName, new Path(pathName), objectName);
    Assert.assertEquals("getFileStatus() shows incorrect path",
            new Path(result), fs.getPath());
  }

  @Test
  public void isSparkOriginTest() throws Exception {
    String mContainerName1 = "cont1";
    ContainerMock mContainer1 = (ContainerMock)new ContainerMock(mAccount,
        mContainerName1).create();

    StoredObjectMock mStoredObject1 = new StoredObjectMock(mContainer1, mContainer1.getName());
    Assert.assertEquals("object already exists",
            false, mStoredObject1.exists());

    mStoredObject1.uploadObject(new byte[]{});
    Assert.assertEquals("object doesn't exists",
            true, mStoredObject1.exists());

    boolean result = Whitebox.invokeMethod(mSwiftAPIClient, "isSparkOrigin", mContainer1.getName());
    Assert.assertEquals("is Spark origin. Expected not.",
            false, result);
  }
}
