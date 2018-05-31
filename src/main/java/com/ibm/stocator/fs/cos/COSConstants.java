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

/**
 * Constants used in the COS REST protocol.
 */
public class COSConstants {
  /*
   * COS name space identifier
   */
  public static final String S3_D = "s3d";
  public static final String S3_A = "s3a";
  public static final String COS = "cos";

  /*
   * COS configuration prefix in the core-site.xml
   */
  public static final String FS_S3_D = "fs." + S3_D;
  public static final String FS_COS = "fs." + COS;
  public static final String FS_S3_A = "fs." + S3_A;
  /*
   * COS configuration key in the core-site.xml
   */
  public static final String S3_D_SERVICE_PREFIX = FS_S3_D + ".";
  public static final String S3_A_SERVICE_PREFIX = FS_S3_A + ".";
  public static final String COS_SERVICE_PREFIX = FS_COS + ".";

  public static final String COS_BUCKET_PROPERTY = FS_S3_D + ".BUCKET-NAME";

  public static final String REGION = ".region.name";
  public static final String REGION_COS_PROPERTY = FS_COS + REGION;

  public static final String ACCESS_KEY = ".access.key";
  public static final String ACCESS_KEY_COS_PROPERTY = FS_COS + ACCESS_KEY;

  public static final String SECRET_KEY = ".secret.key";
  public static final String SECRET_KEY_COS_PROPERTY = FS_COS + SECRET_KEY;

  public static final String ENDPOINT_URL = ".endpoint";
  public static final String ENDPOINT_URL_COS_PROPERTY = FS_COS + ENDPOINT_URL;

  public static final String BLOCK_SIZE = ".block.size";
  public static final String BLOCK_SIZE_COS_PROPERTY = FS_COS + BLOCK_SIZE;

  public static final String AUTO_BUCKET_CREATE = ".create.bucket";
  public static final String AUTO_BUCKET_CREATE_COS_PROPERTY = FS_COS + AUTO_BUCKET_CREATE;

  public static final String V2_SIGNER_TYPE = ".v2.signer.type";
  public static final String V2_SIGNER_TYPE_COS_PROPERTY = FS_COS + V2_SIGNER_TYPE;

  public static final String INPUT_POLICY = ".input.policy";
  public static final String INPUT_POLICY_COS_PROPERTY = FS_COS + INPUT_POLICY;

  public static final String READAHEAD_RANGE = ".readahead.range";
  public static final long DEFAULT_READAHEAD_RANGE = 64 * 1024;

  public static final String SOCKET_TIMEOUT = ".connection.timeout";
  public static final int DEFAULT_SOCKET_TIMEOUT = 800000;

  public static final String SOCKET_SEND_BUFFER = ".socket.send.buffer";
  public static final int DEFAULT_SOCKET_SEND_BUFFER = 8 * 1024;

  public static final String SOCKET_RECV_BUFFER = ".socket.recv.buffer";
  public static final int DEFAULT_SOCKET_RECV_BUFFER = 8 * 1024;

  public static final String MAX_PAGING_KEYS = ".paging.maximum";
  public static final int DEFAULT_MAX_PAGING_KEYS = 5000;

  // the maximum number of threads to allow in the pool used by TransferManager
  public static final String MAX_THREADS = ".threads.max";
  public static final int DEFAULT_MAX_THREADS = 10;

  // the time an idle thread waits before terminating
  public static final String KEEPALIVE_TIME = ".threads.keepalivetime";
  public static final int DEFAULT_KEEPALIVE_TIME = 60;

  //override signature algorithm used for signing requests
  public static final String SIGNING_ALGORITHM = ".signing-algorithm";

  // number of simultaneous connections to cos
  public static final String MAXIMUM_CONNECTIONS = ".connection.maximum";
  public static final int DEFAULT_MAXIMUM_CONNECTIONS = 10000;

  //number of times we should retry errors
  public static final String MAX_ERROR_RETRIES = ".attempts.maximum";
  public static final int DEFAULT_MAX_ERROR_RETRIES = 20;

  //seconds until we give up trying to establish a connection to cos
  public static final String ESTABLISH_TIMEOUT = ".connection.establish.timeout";
  public static final int DEFAULT_ESTABLISH_TIMEOUT = 50000;

  // size of each of or multipart pieces in bytes
  public static final String MULTIPART_SIZE = ".multipart.size";
  public static final long DEFAULT_MULTIPART_SIZE = 8388608; // 8 MB

  // minimum size in bytes before we start a multipart uploads or copy
  public static final String MIN_MULTIPART_THRESHOLD = ".multipart.threshold";
  public static final long DEFAULT_MIN_MULTIPART_THRESHOLD = Integer.MAX_VALUE;

  // the maximum number of tasks cached if all threads are already uploading
  public static final String MAX_TOTAL_TASKS = ".max.total.tasks";
  public static final int DEFAULT_MAX_TOTAL_TASKS = 5;

  //comma separated list of directories
  public static final String BUFFER_DIR = ".buffer.dir";

  // amount of time (in ms) to allow the client to complete the execution of
  // an API call
  public static final String CLIENT_EXEC_TIMEOUT = ".client.execution.timeout";
  public static final int DEFAULT_CLIENT_EXEC_TIMEOUT = 500000;

  // amount of time to wait (in ms) for the request to complete before giving
  // up and timing out
  public static final String REQUEST_TIMEOUT = ".request.timeout";
  public static final int DEFAULT_REQUEST_TIMEOUT = 500000;
  // connect to cos over ssl?
  public static final String SECURE_CONNECTIONS = ".connection.ssl.enabled";
  public static final boolean DEFAULT_SECURE_CONNECTIONS = true;
  //connect to cos through a proxy server?
  public static final String PROXY_HOST = ".proxy.host";
  public static final String PROXY_PORT = ".proxy.port";
  // of the form proxy_ip:proxy_port
  public static final String PROXY_ENV_HTTP_PROXY = ".env.http.proxy";
  public static final String PROXY_ENV_HTTPS_PROXY = ".env.https.proxy";
  public static final String PROXY_USERNAME = ".proxy.username";
  public static final String PROXY_PASSWORD = ".proxy.password";
  public static final String PROXY_DOMAIN = ".proxy.domain";
  public static final String PROXY_WORKSTATION = ".proxy.workstation";

  // User agent prefix
  public static final String USER_AGENT_PREFIX = ".user.agent.prefix";
  public static final String DEFAULT_USER_AGENT_PREFIX = "";

  // community block upload support
  public static final String FAST_UPLOAD_BUFFER_ARRAY = "array";
  public static final String FAST_UPLOAD_BUFFER_DISK = "disk";
  public static final String FAST_UPLOAD_BYTEBUFFER = "bytebuffer";

  public static final String FAST_UPLOAD = ".fast.upload";
  public static final boolean DEFAULT_FAST_UPLOAD = false;

  public static final String FAST_UPLOAD_BUFFER =
      ".fast.upload.buffer";
  public static final String DEFAULT_FAST_UPLOAD_BUFFER =
      FAST_UPLOAD_BUFFER_DISK;

  public static final String FAST_UPLOAD_ACTIVE_BLOCKS =
      ".fast.upload.active.blocks";
  public static final int DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS = 4;

  /** The minimum multipart size which COS supports. */
  public static final int MULTIPART_MIN_SIZE = 5 * 1024 * 1024;
  public static final int MAX_MULTIPART_COUNT = 10000;
  //enable multiobject-delete calls?
  public static final String ENABLE_MULTI_DELETE =
      ".multiobjectdelete.enable";

  // should we try to purge old multipart uploads when starting up
  public static final String PURGE_EXISTING_MULTIPART =
      ".multipart.purge";
  public static final boolean DEFAULT_PURGE_EXISTING_MULTIPART = false;

  // purge any multipart uploads older than this number of seconds
  public static final String PURGE_EXISTING_MULTIPART_AGE =
      ".multipart.purge.age";
  public static final long DEFAULT_PURGE_EXISTING_MULTIPART_AGE = 86400;

  public static final String FLAT_LISTING = ".flat.list";
  public static final boolean DEFAULT_FLAT_LISTING = true;

  public static final String INPUT_FADVISE = "experimental.input.fadvise";
  public static final String INPUT_FADV_NORMAL = "normal";
  public static final String INPUT_FADV_SEQUENTIAL = "sequential";
  public static final String INPUT_FADV_RANDOM = "random";

}
