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

package com.ibm.stocator.fs.swift;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.ConfigurationParseException;
import org.apache.hadoop.conf.Configuration;
import static com.ibm.stocator.fs.common.Constants.SWIFT2D_SERVICE_PREFIX;
import static com.ibm.stocator.fs.common.Constants.SWIFT_SERVICE_PREFIX;
import static com.ibm.stocator.fs.swift.SwiftConstants.AUTH_EXTERNAL_CLASS;
import static com.ibm.stocator.fs.swift.SwiftConstants.KEYSTONE_V3_AUTH;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_AUTH_EXTERNAL_CLASS;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_CONTAINER_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.AUTH_URL;
import static com.ibm.stocator.fs.swift.SwiftConstants.AUTH_METHOD;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_AUTH_METHOD_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_AUTH_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_USERNAME_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PASSWORD_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_TENANT_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_REGION_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PUBLIC_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.USERNAME;
import static com.ibm.stocator.fs.swift.SwiftConstants.BUFFER_DIR;
import static com.ibm.stocator.fs.swift.SwiftConstants.BUFFER_DIR_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.PASSWORD;
import static com.ibm.stocator.fs.swift.SwiftConstants.TENANT;
import static com.ibm.stocator.fs.swift.SwiftConstants.REGION;
import static com.ibm.stocator.fs.swift.SwiftConstants.PUBLIC;
import static com.ibm.stocator.fs.swift.SwiftConstants.BLOCK_SIZE;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_BLOCK_SIZE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.FMODE_AUTOMATIC_DELETE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.FMODE_DELETE_TEMP_DATA;
import static com.ibm.stocator.fs.swift.SwiftConstants.PUBLIC_ACCESS;
import static com.ibm.stocator.fs.swift.SwiftConstants.NON_STREAMING_UPLOAD;
import static com.ibm.stocator.fs.swift.SwiftConstants.NON_STREAMING_UPLOAD_PROPERTY;

/**
 * Integrates Hadoop configuration with the Swift implementation
 */
public final class ConfigurationHandler {

  /**
   * Parse configuration properties from the core-site.xml and initialize
   * Swift configuration
   * @param uri uri of the file system
   * @param conf configuration
   * @return parsed configuration for the Swift driver
   * @throws ConfigurationParseException is failed to parse configuration
   */
  public static Properties initialize(URI uri, Configuration conf) throws IOException,
      ConfigurationParseException {
    String host = Utils.getHost(uri);
    Properties props = new Properties();
    if (!Utils.validSchema(uri)) {
      props.setProperty(SWIFT_AUTH_METHOD_PROPERTY, PUBLIC_ACCESS);
    } else {
      final String container = Utils.getContainerName(host);
      final String service = Utils.getServiceName(host);
      final String[] prefix = new String[]{SWIFT_SERVICE_PREFIX + service};
      final String prefix2D = SWIFT2D_SERVICE_PREFIX + service;
      props.setProperty(SWIFT_CONTAINER_PROPERTY, container);
      Utils.updateProperty(conf, prefix2D, prefix, AUTH_URL, props, SWIFT_AUTH_PROPERTY, true);
      Utils.updateProperty(conf, prefix2D, prefix, USERNAME, props, SWIFT_USERNAME_PROPERTY, true);
      Utils.updateProperty(conf, prefix2D, prefix, PASSWORD, props, SWIFT_PASSWORD_PROPERTY, true);
      Utils.updateProperty(conf, prefix2D, prefix, BUFFER_DIR, props, BUFFER_DIR_PROPERTY, false);
      Utils.updateProperty(conf, prefix2D, prefix, NON_STREAMING_UPLOAD, props,
          NON_STREAMING_UPLOAD_PROPERTY, false);
      Utils.updateProperty(conf, prefix2D, prefix, AUTH_METHOD, props, SWIFT_AUTH_METHOD_PROPERTY,
          false);
      Utils.updateProperty(conf, prefix2D, prefix, BLOCK_SIZE, props, SWIFT_BLOCK_SIZE_PROPERTY,
          false);
      Utils.updateProperty(conf, prefix2D, prefix, FMODE_DELETE_TEMP_DATA, props,
          FMODE_AUTOMATIC_DELETE_PROPERTY, false);
      Utils.updateProperty(conf, prefix2D, prefix, PUBLIC, props, SWIFT_PUBLIC_PROPERTY, false);
      String authMethod = props.getProperty(SWIFT_AUTH_METHOD_PROPERTY, KEYSTONE_V3_AUTH);
      props.setProperty(SWIFT_AUTH_METHOD_PROPERTY, authMethod);

      Utils.updateProperty(conf, prefix2D, prefix, TENANT, props, SWIFT_TENANT_PROPERTY, false);
      Utils.updateProperty(conf, prefix2D, prefix, REGION, props, SWIFT_REGION_PROPERTY, true);
      Utils.updateProperty(conf, prefix2D, prefix, AUTH_EXTERNAL_CLASS, props,
          SWIFT_AUTH_EXTERNAL_CLASS, false);
    }
    return props;
  }
}
