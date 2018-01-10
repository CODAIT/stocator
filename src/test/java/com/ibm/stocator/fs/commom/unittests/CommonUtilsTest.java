 package com.ibm.stocator.fs.commom.unittests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;

import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.InvalidContainerNameException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Utils.class)
public class CommonUtilsTest {
  
  public CommonUtilsTest() throws URISyntaxException {}

  private Utils mUtils;
  private Configuration mConf;
  private String mPrefix;
  private String[] mAltPrefix;
  private String mKey;
  private Properties mProps;
  private String mPropsKey;
  private boolean mRequired;

  private static final String[] HOST_LIST_VALID = {
      "cos://abc.service/a/b",
      "cos://abc.service",
      "cos://abc.service/",
      "cos://bucket",
      "cos://a.b.c.service"};

  private static final String[] HOST_LIST_INVALID = {"cos://abc.service/a/b",
      "cos://ab.service",
      "cos://127.0.0.1/bucket",
      "cos://a"};
  
  private static final String[] PATH = {"cos://bucket.service/object?token=aabbcc",
            "cos://bucket.service/object?token=aabbcc/1234"};
      
  @Before
  public final void before() {
    mUtils = PowerMockito.mock(Utils.class);

    mConf = Mockito.mock(Configuration.class);
    mPrefix = "fs.cos.service";
    mAltPrefix = new String[] {"fs.s3a.service", "fs.s3d.service"};
    mKey = ".iam.token";
    mProps = PowerMockito.spy(new Properties());
    mPropsKey = "fs.cos.iam.token.iam.token";
    mRequired = false;

    Mockito.when((mConf.get("fs.cos.service.iam.token"))).thenReturn("123");
  }

  @Test
  public void testHostExtraction() throws Exception {
    String host = Utils.getHost(new URI(HOST_LIST_VALID[0]));
    Assert.assertEquals("Invalid host name extracted",
        "abc.service", host);
    host = Utils.getHost(new URI(HOST_LIST_VALID[1]));
    Assert.assertEquals("Invalid host name extracted",
        "abc.service", host);
    host = Utils.getHost(new URI(HOST_LIST_VALID[2]));
    Assert.assertEquals("Invalid host name extracted",
        "abc.service", host);
    host = Utils.getHost(new URI(HOST_LIST_VALID[3]));
    Assert.assertEquals("Invalid host name extracted",
        "bucket", host);
  }
  
  @Test
  public void testIAMTokenExtractionPath() throws IOException {
    String token = Utils.extractToken(new Path(PATH[0]));
    Assert.assertEquals("aabbcc", token);
  }

  @Test
  public void testIAMTokenExtractionPath2() throws IOException {
    String token = Utils.extractToken(new Path(PATH[1]));
    Assert.assertEquals("aabbcc", token);
  }

  @Test
  public void removeTokenTestString() {
    String token = Utils.removeToken(PATH[0]);
    Assert.assertEquals("cos://bucket.service/object", token);
  }

  @Test
  public void removeTokenTestString2() {
    String token = Utils.removeToken(PATH[1]);
    Assert.assertEquals("cos://bucket.service/object/1234", token);
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testContainerExtraction() {
    try {
      String host = Utils.getHost(new URI(HOST_LIST_INVALID[0]));
      String container = Utils.getContainerName(host);
      exception.expect(InvalidContainerNameException.class);
      Utils.validContainer(container);

      host = Utils.getHost(new URI(HOST_LIST_INVALID[1]));
      container = Utils.getContainerName(host);
      exception.expect(InvalidContainerNameException.class);
      Utils.validContainer(container);

      host = Utils.getHost(new URI(HOST_LIST_INVALID[2]));
      container = Utils.getContainerName(host);
      exception.expect(InvalidContainerNameException.class);
      Utils.validContainer(container);

      host = Utils.getHost(new URI(HOST_LIST_VALID[4]));
      container = Utils.getContainerName(host);
      Assert.assertEquals(container, "a.b.c");
    } catch (IOException e) {
      Assert.fail();
    } catch (URISyntaxException e) {
      Assert.fail();
    }
  }

}
