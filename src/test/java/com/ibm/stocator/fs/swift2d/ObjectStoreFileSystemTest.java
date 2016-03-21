package com.ibm.stocator.fs.swift2d;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import com.ibm.stocator.fs.ObjectStoreFileSystem;
import com.ibm.stocator.fs.common.Constants;

public class ObjectStoreFileSystemTest {

  private ObjectStoreFileSystem mMockObjectStoreFileSystem;
  private String hostName = "swift2d://out1003.lvm";

  @Before
  public final void before() {
    mMockObjectStoreFileSystem = PowerMockito.mock(ObjectStoreFileSystem.class);
    Whitebox.setInternalState(mMockObjectStoreFileSystem, "hostNameScheme", hostName);
  }

  @Test
  public void getObjectNameTest() throws Exception {
    Path input = new Path("swift2d://out1003.lvm/a/b/c/m.data/_temporary/"
        + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    String result = Whitebox.invokeMethod(mMockObjectStoreFileSystem, "getObjectName", input,
        Constants.HADOOP_TEMPORARY, true);
    Assert.assertEquals("/a/b/c/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

    input = new Path("swift2d://out1003.lvm/a/b/m.data/_temporary/"
        + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    result = Whitebox.invokeMethod(mMockObjectStoreFileSystem, "getObjectName", input,
        Constants.HADOOP_TEMPORARY, true);
    Assert.assertEquals("/a/b/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

    input = new Path("swift2d://out1003.lvm/m.data/_temporary/"
        + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    result = Whitebox.invokeMethod(mMockObjectStoreFileSystem, "getObjectName", input,
        Constants.HADOOP_TEMPORARY, true);
    Assert.assertEquals("/m.data/part-00099-attempt_201603141928_0000_m_000099_102", result);

  }

  @Test(expected = IOException.class)
  public void getObjectWrongNameTest() throws Exception {
    Path input = new Path("swift2d://out1003.lvm_temporary/"
        + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.invokeMethod(mMockObjectStoreFileSystem, "getObjectName", input,
        Constants.HADOOP_TEMPORARY, true);

    input = new Path("swift2d://out1003.lvm/temporary/"
        + "0/_temporary/attempt_201603141928_0000_m_000099_102/part-00099");
    Whitebox.invokeMethod(mMockObjectStoreFileSystem, "getObjectName", input,
        Constants.HADOOP_TEMPORARY, true);
  }
}
