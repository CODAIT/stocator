/**
 * (C) Copyright IBM Corp. 2015, 2016
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.s3;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Utils;

import static com.ibm.stocator.fs.common.Constants.S3_SERVICE_PREFIX;
import static com.ibm.stocator.fs.s3.S3Constants.ACCESS_KEY;
import static com.ibm.stocator.fs.s3.S3Constants.BLOCK_SIZE;
import static com.ibm.stocator.fs.s3.S3Constants.BUCKET;
import static com.ibm.stocator.fs.s3.S3Constants.REGION;
import static com.ibm.stocator.fs.s3.S3Constants.S3_ACCESS_KEY_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_BLOCK_SIZE_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_BUCKET_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_CONTAINER_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_REGION_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.S3_SECRET_KEY_PROPERTY;
import static com.ibm.stocator.fs.s3.S3Constants.SECRET_KEY;

/**
 * Integrates Hadoop configuration with the Swift implementation
 */
public final class ConfigurationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationHandler.class);

  /**
   * Parse configuration properties from the core-site.xml and initialize
   * Swift configuration
   * @param uri uri of the file system
   * @param conf configuration
   * @return parsed configuration for the Swift driver
   * @throws IOException if the configuration is invalid
   */
  public static Properties initialize(URI uri, Configuration conf) throws IOException {
    LOG.debug("S3 driver: initialize start");

    String prefix = S3_SERVICE_PREFIX + S3_CONTAINER_PROPERTY;
    LOG.debug("Filesystem {}, using conf keys {}", uri, prefix);
    Properties props = new Properties();
    props.setProperty(S3_CONTAINER_PROPERTY, "s3");
    Utils.updateProperty(conf, prefix, BLOCK_SIZE, props, S3_BLOCK_SIZE_PROPERTY, false);
    Utils.updateProperty(conf, prefix, REGION, props, S3_REGION_PROPERTY, false);
    Utils.updateProperty(conf, prefix, BUCKET, props, S3_BUCKET_PROPERTY, true);
    Utils.updateProperty(conf, prefix, ACCESS_KEY, props, S3_ACCESS_KEY_PROPERTY, true);
    Utils.updateProperty(conf, prefix, SECRET_KEY, props, S3_SECRET_KEY_PROPERTY, true);

    LOG.debug("Initialize completed successfully");
    return props;
  }

}
