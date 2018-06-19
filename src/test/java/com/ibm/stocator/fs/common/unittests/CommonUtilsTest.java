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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;

import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.InvalidContainerNameException;

import static com.ibm.stocator.fs.cos.COSConstants.FS_S3_A;
import static com.ibm.stocator.fs.cos.COSConstants.FS_S3_D;
import static com.ibm.stocator.fs.cos.COSConstants.FS_COS;

public class CommonUtilsTest {

  private static final String[] FS_ALT_KEYS = new String[]{FS_S3_D, FS_S3_A};

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

  @Test
  public void testAlternativeKeys() {
    Configuration conf = new Configuration(false);
    conf.set("fs.cos.key1", "value1");
    conf.set("fs.s3a.key2", "value2");
    conf.set("fs.s3d.key3", "value3");
    conf.set("fs.cos.key4", "value4");
    conf.set("fs.s3a.key4", "value41");
    conf.set("fs.cos.key5", "value5");
    conf.set("fs.s3d.key5", "value51");
    conf.set("fs.s3d.key6", "value6");
    conf.set("fs.s3a.key6", "value61");

    conf.set("fs.cos.key1i", "1");
    conf.set("fs.s3a.key2i", "2");
    conf.set("fs.s3d.key3i", "3");
    conf.set("fs.cos.key4i", "4");
    conf.set("fs.s3a.key4i", "41");
    conf.set("fs.cos.key5i", "5");
    conf.set("fs.s3d.key5i", "51");
    conf.set("fs.s3d.key6i", "6");
    conf.set("fs.s3a.key6i", "61");

    Assert.assertEquals("fs.cos.key1", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key1"));
    Assert.assertEquals("fs.s3a.key2", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key2"));
    Assert.assertEquals("fs.s3d.key3", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key3"));
    Assert.assertEquals("fs.cos.key4", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key4"));
    Assert.assertEquals("fs.cos.key5", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key5"));
    Assert.assertEquals("fs.s3d.key6", Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key6"));
    Assert.assertNull(Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS, ".key7"));

    Assert.assertEquals("value1", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key1"));
    Assert.assertEquals("value2", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key2"));
    Assert.assertEquals("value3", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key3"));
    Assert.assertEquals("value4", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key4"));
    Assert.assertEquals("value5", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key5"));
    Assert.assertEquals("value6", Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key6"));
    Assert.assertNull(Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, ".key7"));

    Assert.assertEquals(1, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key1i", -1));
    Assert.assertEquals(2, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key2i", -1));
    Assert.assertEquals(3, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key3i", -1));
    Assert.assertEquals(4, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key4i", -1));
    Assert.assertEquals(5, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key5i", -1));
    Assert.assertEquals(6, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key6i", -1));
    Assert.assertEquals(-1, Utils.getInt(conf, FS_COS, FS_ALT_KEYS, ".key67i", -1));

    Assert.assertEquals(1, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key1i", -1));
    Assert.assertEquals(2, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key2i", -1));
    Assert.assertEquals(3, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key3i", -1));
    Assert.assertEquals(4, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key4i", -1));
    Assert.assertEquals(5, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key5i", -1));
    Assert.assertEquals(6, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key6i", -1));
    Assert.assertEquals(-1, Utils.getLong(conf, FS_COS, FS_ALT_KEYS, ".key67i", -1));
  }

}
