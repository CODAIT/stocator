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

/**
 * General constants used in the code
 */
public class Constants {

  /*
   * Swift name space identifier
   */
  public static final String SWIFT = "swift";
  /*
   * Swift2d configuration prefix in the core-site.xml
   */
  public static final String FS_SWIFT = "fs." + SWIFT;
  /*
   * Swift2d configuration key in the core-site.xml
   */
  public static final String SWIFT_SERVICE_PREFIX = FS_SWIFT + ".service.";
  /*
   * Swift2d name space identifier
   */
  public static final String SWIFT2D = "swift2d";
  /*
   * Swift2d configuration prefix in the core-site.xml
   */
  public static final String FS_SWIFT2D = "fs." + SWIFT2D;
  /*
   * Swift2d configuration key in the core-site.xml
   */
  public static final String SWIFT2D_SERVICE_PREFIX = FS_SWIFT2D + ".service.";
  /*
   * Hadoop identification for the temporary directory
   */
  public static final String HADOOP_TEMPORARY = "_temporary";
  /*
   * Hadoop identification for TASK_ID attempt
   */
  public static final String HADOOP_ATTEMPT = "attempt_";
  /*
   * Hadoop _SUCCESS object
   */
  public static final String HADOOP_SUCCESS = "_SUCCESS";

  /*
   * User agent for HTTP requests
   */
  public static final String USER_AGENT_HTTP_HEADER = "User-Agent";

  /*
   * Stocator user agent identifier
   */
  public static final String STOCATOR_USER_AGENT = "stocator";

  /*
   * HTTP Range header
   */
  public static final String RANGES_HTTP_HEADER = "Range";

  /*
   * Read ahead range
   */
  public static final long DEFAULT_READAHEAD_RANGE = 64 * 1024;
  /*
   * Normal read strategy
   */
  public static final String NORMAL_READ_STRATEGY = "Normal";
  /*
   * Read current block or the current position + readahead
   */
  public static final String RANDOM_READ_STRATEGY = "Random";
  /*
   * The goal to read the entire object
   */
  public static final String SEQ_READ_STRATEGY = "Sequential";

  /*
   * The data that is buffered in memory before opening the HTTP PUT request
   * and flushing the data
   */
  public static final int SWIFT_DATA_BUFFER = 64 * 1024;
  /*
   * directory mime type
   */
  public static final String APPLICATION_DIRECTORY = "application/directory";
  /*
   * maximal connections per IP route
   */
  public static final String MAX_PER_ROUTE = "fs.stocator.MaxPerRoute";
  /*
   * maximal concurrent connections
   */
  public static final String MAX_TOTAL_CONNECTIONS = "fs.stocator.MaxTotal";
  /*
   * low level socket timeout in milliseconds
   */
  public static final String SOCKET_TIMEOUT = "fs.stocator.SoTimeout";
  /*
   * number of retries for certain HTTP issues
   */
  public static final String EXECUTION_RETRY = "fs.stocator.executionCount";
  /*
   * Request level connect timeout
   * Determines the timeout in milliseconds until a connection is established
   */
  public static final String REQUEST_CONNECT_TIMEOUT = "fs.stocator.ReqConnectTimeout";
  /*
   * Request level connection timeout
   * Returns the timeout in milliseconds used when requesting a connection from the
   * connection manager
   */
  public static final String REQUEST_CONNECTION_TIMEOUT = "fs.stocator."
      + "ReqConnectionRequestTimeout";
  /*
   * Defines the socket timeout (SO_TIMEOUT) in milliseconds,
   * which is the timeout for waiting for data or, put differently,
   * a maximum period inactivity between two consecutive data packets).
   */
  public static final String REQUEST_SOCKET_TIMEOUT = "fs.stocator.ReqSocketTimeout";
  /*
   * JOSS synchronize with server time
   */
  public static final String JOSS_SYNC_SERVER_TIME = "fs.stocator.joss.synchronize.time";
  public static final String OUTPUT_COMMITTER_TYPE = "fs.stocator.committer.type";
  public static final String DEFAULT_FOUTPUTCOMMITTER_V1 = "defaultFOutputV1";
  public static final String TRASH_FOLDER = ".Trash";
  public static final String CACHE_SIZE = "fs.stocator.cache.size";
  public static final int GUAVA_CACHE_SIZE_DEFAULT = 2000;
  public static final String FS_STOCATOR_GLOB_BRACKET_SUPPORT = "fs.stocator.glob.bracket.support";
  public static final String FS_STOCATOR_GLOB_BRACKET_SUPPORT_DEFAULT = "false";
  public static final String FS_STOCATOR_FMODE_DATA_CLEANUP = "fs.stocator.failure.data.cleanup";
  public static final String FS_STOCATOR_FMODE_DATA_CLEANUP_DEFAULT = "false";

  /*
   * Hadoop identification for PART
   */
  public static final String HADOOP_PART = "part-";

}
