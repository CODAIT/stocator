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
