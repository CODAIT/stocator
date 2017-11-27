package com.ibm.stocator.fs.cos.unittests;

import java.io.IOException;

import org.junit.Test;

import com.ibm.stocator.fs.common.Utils;

import junit.framework.Assert;

public class COSURITest {

  @Test
  public void standardBucketNameAndServiceTest() throws IOException {
    String hostname = "aa-bb-cc.service";
    String service = Utils.getServiceName(hostname);
    Assert.assertEquals("service", service);
  }

  @Test
  public void bucketNameWithDotsAndServiceTest() throws IOException {
    String hostname = "aa.bb.cc.service";
    String service = Utils.getServiceName(hostname);
    Assert.assertEquals("service", service);
  }

  @Test
  public void bucketNameWithDotsAndNoServiceTest() throws IOException {
    String hostname = "aa.bb.cc";
    String service = Utils.getServiceName(hostname);
    Assert.assertNotSame("service", service);
  }

}
