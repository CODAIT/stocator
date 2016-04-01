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

import org.apache.hadoop.conf.Configuration;

import static com.ibm.stocator.fs.common.Constants.SWIFT_SERVICE_PREFIX;
import static com.ibm.stocator.fs.swift.SwiftConstants.KEYSTONE_V3_AUTH;
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
import static com.ibm.stocator.fs.swift.SwiftConstants.PASSWORD;
import static com.ibm.stocator.fs.swift.SwiftConstants.TENANT;
import static com.ibm.stocator.fs.swift.SwiftConstants.REGION;
import static com.ibm.stocator.fs.swift.SwiftConstants.PUBLIC;
import static com.ibm.stocator.fs.swift.SwiftConstants.BLOCK_SIZE;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_BLOCK_SIZE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.OBJECT_SIZE;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_OBJECT_SIZE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PROJECT_ID_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_USER_ID_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.FMODE_AUTOMATIC_DELETE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.FMODE_DELETE_TEMP_DATA;
import static com.ibm.stocator.fs.swift.SwiftConstants.PUBLIC_ACCESS;

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
   * @throws IOException if the configuration is invalid
   */
  public static Properties initialize(URI uri, Configuration conf) throws IOException {
    String host = Utils.getHost(uri);
    Properties props = new Properties();
    if (!Utils.validSchema(uri)) {
      props.setProperty(SWIFT_AUTH_METHOD_PROPERTY, PUBLIC_ACCESS);
    } else {
      String container = Utils.getContainerName(host);
      String service = Utils.getServiceName(host);
      String prefix = SWIFT_SERVICE_PREFIX + service;
      props.setProperty(SWIFT_CONTAINER_PROPERTY, container);
      Utils.updateProperty(conf, prefix, AUTH_URL, props, SWIFT_AUTH_PROPERTY, true);
      Utils.updateProperty(conf, prefix, USERNAME, props, SWIFT_USERNAME_PROPERTY, true);
      Utils.updateProperty(conf, prefix, PASSWORD, props, SWIFT_PASSWORD_PROPERTY, true);
      Utils.updateProperty(conf, prefix, TENANT, props, SWIFT_TENANT_PROPERTY, true);
      Utils.updateProperty(conf, prefix, AUTH_METHOD, props, SWIFT_AUTH_METHOD_PROPERTY, false);
      Utils.updateProperty(conf, prefix, BLOCK_SIZE, props, SWIFT_BLOCK_SIZE_PROPERTY, false);
      Utils.updateProperty(conf, prefix, OBJECT_SIZE, props, SWIFT_OBJECT_SIZE_PROPERTY, false);
      Utils.updateProperty(conf, prefix, FMODE_DELETE_TEMP_DATA, props,
          FMODE_AUTOMATIC_DELETE_PROPERTY, false);
      Utils.updateProperty(conf, prefix, PUBLIC, props, SWIFT_PUBLIC_PROPERTY, false);
      String authMethod = props.getProperty(SWIFT_AUTH_METHOD_PROPERTY, KEYSTONE_V3_AUTH);
      props.setProperty(SWIFT_AUTH_METHOD_PROPERTY, authMethod);
      if (authMethod.equals(KEYSTONE_V3_AUTH)) {
        Utils.updateProperty(conf, prefix, REGION, props, SWIFT_REGION_PROPERTY, false);
        props.setProperty(SWIFT_PROJECT_ID_PROPERTY, props.getProperty(SWIFT_TENANT_PROPERTY));
        props.setProperty(SWIFT_USER_ID_PROPERTY, props.getProperty(SWIFT_USERNAME_PROPERTY));
      } else {
        Utils.updateProperty(conf, prefix,  REGION, props, SWIFT_REGION_PROPERTY, false);
      }
    }
    return props;
  }
}
