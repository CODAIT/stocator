package com.ibm.stocator.fs.commom.unittests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Assert;
import org.junit.Rule;

import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.InvalidContainerNameException;

public class CommonUtilsTest {

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
