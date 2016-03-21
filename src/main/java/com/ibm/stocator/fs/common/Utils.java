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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;

public class Utils {

  public static final String BAD_HOST = " hostname '%s' must be in the form container.service";

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
   * @throws IOException
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
   * Extract host name from the URI
   *
   * @param uri
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
   * @param key key in the configuration file
   * @param props destination property set
   * @param propsKey key in the property set
   * @param required if the key is mandatory
   * @throws IOException if there was no match for the key
   */

  public static void updateProperty(Configuration conf, String prefix, String key,
      Properties props, String propsKey, boolean required)
      throws IOException {
    String val = conf.get(prefix + key);
    if (required && val == null) {
      throw new IOException("Missing mandatory configuration: " + key);
    }
    if (val != null) {
      props.setProperty(propsKey, val.trim());
    }
  }

  /**
   * Extract Hadoop Task ID from path
   * @param path
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

}
