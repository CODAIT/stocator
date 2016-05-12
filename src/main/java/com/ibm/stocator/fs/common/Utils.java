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

package com.ibm.stocator.fs.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.SwiftAPIClient;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;

public class Utils {

  public static final String BAD_HOST = " hostname '%s' must be in the form container.service";

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftAPIClient.class);

  /**
   * IOException if the host name is not comply with container.service
   *
   * @param hostname hostname
   * @return IOException
   */
  private static IOException badHostName(String hostname) {
    return new IOException(String.format(BAD_HOST, hostname));
  }

  /**
   * Extracts container name from the container.service
   *
   * @param hostname hostname to split
   * @return the container
   * @throws IOException if hostname is invalid
   */
  public static String getContainerName(String hostname) throws IOException {
    int i = hostname.indexOf(".");
    if (i <= 0) {
      throw badHostName(hostname);
    }
    return hostname.substring(0, i);
  }

  /**
   * Extracts service name from the container.service
   *
   * @param hostname hostname
   * @return the separated out service name
   * @throws IOException if the hostname was invalid
   */
  public static String getServiceName(String hostname) throws IOException {
    int i = hostname.indexOf(".");
    if (i <= 0) {
      throw badHostName(hostname);
    }
    String service = hostname.substring(i + 1);
    if (service.isEmpty() || service.contains(".")) {
      throw badHostName(hostname);
    }
    return service;
  }

  /**
   * Test if hostName of the form container.service
   *
   * @param uri schema URI
   * @return true if hostName of the form container.service
   */
  public static boolean validSchema(URI uri) {
    String hostName = Utils.getHost(uri);
    int i = hostName.indexOf(".");
    if (i < 0) {
      return false;
    }
    String service = hostName.substring(i + 1);
    if (service.isEmpty() || service.contains(".")) {
      return false;
    }
    return true;
  }

  public static boolean validSchema(String path) throws IOException {
    try {
      return validSchema(new URI(path));
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Extract host name from the URI
   *
   * @param uri object store uri
   * @return host name
   */
  public static String getHost(URI uri) {
    String host = uri.getHost();
    if (host != null) {
      return host;
    }
    host = uri.toString();
    int sInd = host.indexOf("//") + 2;
    host = host.substring(sInd);
    int eInd = host.indexOf("/");
    host = host.substring(0,eInd);
    return host;
  }

  /**
   * Get a mandatory configuration option
   *
   * @param props property set
   * @param key key
   * @return value of the configuration
   * @throws IOException if there was no match for the key
   */
  public static String getOption(Properties props, String key) throws IOException {
    String val = props.getProperty(key);
    if (val == null) {
      throw new IOException("Undefined property: " + key);
    }
    return val;
  }

  /**
   * Read key from core-site.xml and parse it to Swift configuration
   *
   * @param conf source configuration
   * @param prefix configuration key prefix
   * @param alternativePrefix alternative prefix
   * @param key key in the configuration file
   * @param props destination property set
   * @param propsKey key in the property set
   * @param required if the key is mandatory
   * @throws IOException if there was no match for the key
   */

  public static void updateProperty(Configuration conf, String prefix, String alternativePrefix,
      String key, Properties props, String propsKey, boolean required) throws IOException {
    String val = conf.get(prefix + key);
    if (val == null) {
      // try alternative key
      val = conf.get(alternativePrefix + key);
      LOG.trace("Trying alternative key {}{}", alternativePrefix, key);
    }
    if (required && val == null) {
      throw new IOException("Missing mandatory configuration: " + key);
    }
    if (val != null) {
      props.setProperty(propsKey, val.trim());
    }
  }

  /**
   * Extract Hadoop Task ID from path
   * @param path path to extract attempt id
   * @return task id
   */
  public static String extractTaskID(String path) {
    if (path.contains(HADOOP_ATTEMPT)) {
      String prf = path.substring(path.indexOf(HADOOP_ATTEMPT));
      if (prf.contains("/")) {
        return TaskAttemptID.forName(prf.substring(0, prf.indexOf("/"))).toString();
      }
      return TaskAttemptID.forName(prf).toString();
    }
    return null;
  }

  /**
   * Transform http://hostname/v1/auth_id/container/object to
   * http://hostname/v1/auth_id
   *
   * @param publicURL public url
   * @return accessURL access url
   * @throws IOException if path is malformed
   */
  public static String extractAccessURL(String publicURL) throws IOException {
    try {
      String hostName = new URI(publicURL).getAuthority();
      int  start = publicURL.indexOf("//") + 2 + hostName.length() + 1;
      for (int i = 0; i < 2; i++) {
        start = publicURL.indexOf("/", start) + 1;
      }
      String authURL = publicURL.substring(0, start);
      if (authURL.endsWith("/")) {
        authURL = authURL.substring(0, authURL.length() - 1);
      }
      return authURL;
    } catch (URISyntaxException e) {
      throw new IOException("Public URL: " + publicURL + " is not valid");
    }
  }

  /**
   * Extracts container name from http://hostname/v1/auth_id/container/object
   *
   * @param publicURL public url
   * @param accessURL access url
   * @return container name
   */
  public static String extractDataRoot(String publicURL, String accessURL) {
    String reminder = publicURL.substring(accessURL.length() + 1);
    String container = null;
    if (reminder.indexOf("/") > 0) {
      container =  reminder.substring(0, reminder.indexOf("/"));
    } else {
      container = reminder;
    }
    if (container.endsWith("/")) {
      container = container.substring(0, container.length() - 1);
    }
    return container;
  }

  /**
   * Extracts container/object  from http://hostname/v1/auth_id/container/object
   *
   * @param publicURL pubic url
   * @param accessURL access url
   * @return reminder of the URI
   */
  public static String extractReminder(String publicURL, String accessURL) {
    return publicURL.substring(accessURL.length());
  }
}
