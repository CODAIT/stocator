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

package com.ibm.stocator.fs.cos;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

import com.ibm.stocator.fs.common.Utils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ibm.stocator.fs.cos.COSConstants.ACCESS_KEY;
import static com.ibm.stocator.fs.cos.COSConstants.ACCESS_KEY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.SECRET_KEY;
import static com.ibm.stocator.fs.cos.COSConstants.SECRET_KEY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.ENDPOINT_URL;
import static com.ibm.stocator.fs.cos.COSConstants.ENDPOINT_URL_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.AUTO_BUCKET_CREATE;
import static com.ibm.stocator.fs.cos.COSConstants.AUTO_BUCKET_CREATE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.BLOCK_SIZE;
import static com.ibm.stocator.fs.cos.COSConstants.BLOCK_SIZE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_POLICY;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_POLICY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.REGION;
import static com.ibm.stocator.fs.cos.COSConstants.REGION_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.COS_BUCKET_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.S3_A_SERVICE_PREFIX;
import static com.ibm.stocator.fs.cos.COSConstants.S3_D_SERVICE_PREFIX;
import static com.ibm.stocator.fs.cos.COSConstants.COS_SERVICE_PREFIX;
import static com.ibm.stocator.fs.cos.COSConstants.V2_SIGNER_TYPE;
import static com.ibm.stocator.fs.cos.COSConstants.V2_SIGNER_TYPE_COS_PROPERTY;

/**
 * Integrates Hadoop configuration with the COS client implementation
 */
public final class ConfigurationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationHandler.class);

  /**
   * Parse configuration properties from the core-site.xml and initialize
   * COS configuration
   * @param uri uri of the file system
   * @param conf configuration
   * @param scheme connector supposed scheme
   * @return parsed configuration for the COS driver
   * @throws IOException if the configuration is invalid
   */
  public static Properties initialize(URI uri, Configuration conf,
      String scheme) throws IOException {
    LOG.debug("COS driver: initialize start for {} ", uri.toString());
    String host = Utils.getHost(uri);
    LOG.debug("extracted host name from {} is {}", uri.toString(), host);
    String bucket = Utils.getContainerName(host, false);
    String service = null;
    try {
      service = Utils.getServiceName(host);
    } catch (IOException ex) {
      LOG.warn("Failed to extract service from the host {}", host);
      throw new IOException(ex);
    }
    if (service == null) {
      service =  "service";
    }
    LOG.debug("Initiaize for bucket: {}, service: {}", bucket , service);
    String[] altPrefix = new String[]{S3_A_SERVICE_PREFIX + service,
        S3_D_SERVICE_PREFIX + service};
    String prefix = COS_SERVICE_PREFIX + service;
    LOG.debug("Filesystem {}, using conf keys for {}. Alternative list {}", uri,
        prefix, Arrays.toString(altPrefix));
    Properties props = new Properties();
    props.setProperty(COS_BUCKET_PROPERTY, bucket);
    Utils.updateProperty(conf, prefix, altPrefix, ACCESS_KEY, props,
        ACCESS_KEY_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, SECRET_KEY, props,
        SECRET_KEY_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, ENDPOINT_URL, props,
        ENDPOINT_URL_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, AUTO_BUCKET_CREATE, props,
        AUTO_BUCKET_CREATE_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, V2_SIGNER_TYPE, props,
        V2_SIGNER_TYPE_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, INPUT_POLICY, props,
        INPUT_POLICY_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, BLOCK_SIZE, props,
        BLOCK_SIZE_COS_PROPERTY, false);
    Utils.updateProperty(conf, prefix, altPrefix, REGION, props,
        REGION_COS_PROPERTY, false);
    LOG.debug("Initialize completed successfully for bucket {} service {}", bucket, service);
    return props;
  }

}
