package com.ibm.stocator.fs.common.unittests;

import org.junit.Assert;

import org.junit.Test;

import com.ibm.stocator.fs.cos.COSUtils;

public class COSUtilsTest {

  @Test
  public void testToken() {
    String token = "123";
    String hostName = "cos://bucket.service/";
    String path =  hostName + "aa";
    String pathWithToken = COSUtils.addTokenToPath(path, token, hostName);
    String extractToken = COSUtils.extractToken(pathWithToken);
    Assert.assertEquals("Token extract failed", extractToken, token);
    String originalPath = COSUtils.removeToken(pathWithToken);
    Assert.assertEquals("Remove token failed", path, originalPath);
  }
}
