/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  (C) Copyright IBM Corp. 2015, 2016
 */

package com.ibm.stocator.fs.cos;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import com.ibm.stocator.fs.cache.MemoryCache;
import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.StocatorPath;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.ConfigurationParseException;
import com.ibm.stocator.fs.cos.ConfigurationHandler;
import com.ibm.stocator.fs.cos.OnetimeInitialization;
import com.ibm.stocator.fs.cos.COSInputStream;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import static com.ibm.stocator.fs.common.Constants.HADOOP_SUCCESS;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.CACHE_SIZE;
import static com.ibm.stocator.fs.common.Constants.GUAVA_CACHE_SIZE_DEFAULT;
import static com.ibm.stocator.fs.cos.COSConstants.CLIENT_EXEC_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_CLIENT_EXEC_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_ESTABLISH_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_KEEPALIVE_TIME;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MAXIMUM_CONNECTIONS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MAX_ERROR_RETRIES;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MAX_THREADS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MAX_TOTAL_TASKS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MIN_MULTIPART_THRESHOLD;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MULTIPART_SIZE;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_REQUEST_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_SECURE_CONNECTIONS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_SOCKET_RECV_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_SOCKET_SEND_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_SOCKET_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.FS_COS;
import static com.ibm.stocator.fs.cos.COSConstants.FS_S3_A;
import static com.ibm.stocator.fs.cos.COSConstants.FS_S3_D;
import static com.ibm.stocator.fs.cos.COSConstants.ESTABLISH_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.KEEPALIVE_TIME;
import static com.ibm.stocator.fs.cos.COSConstants.MAXIMUM_CONNECTIONS;
import static com.ibm.stocator.fs.cos.COSConstants.MAX_ERROR_RETRIES;
import static com.ibm.stocator.fs.cos.COSConstants.MAX_THREADS;
import static com.ibm.stocator.fs.cos.COSConstants.MAX_TOTAL_TASKS;
import static com.ibm.stocator.fs.cos.COSConstants.MIN_MULTIPART_THRESHOLD;
import static com.ibm.stocator.fs.cos.COSConstants.MULTIPART_SIZE;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_DOMAIN;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_HOST;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_PASSWORD;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_PORT;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_USERNAME;
import static com.ibm.stocator.fs.cos.COSConstants.PROXY_WORKSTATION;
import static com.ibm.stocator.fs.cos.COSConstants.REQUEST_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.AUTO_BUCKET_CREATE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.ACCESS_KEY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.SECRET_KEY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.BLOCK_SIZE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.COS_BUCKET_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.ENDPOINT_URL_COS_PROPERTY;
import static com.ibm.stocator.fs.common.Constants.FS_STOCATOR_FMODE_DATA_CLEANUP;
import static com.ibm.stocator.fs.cos.COSConstants.REGION_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.V2_SIGNER_TYPE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.SECURE_CONNECTIONS;
import static com.ibm.stocator.fs.cos.COSConstants.SIGNING_ALGORITHM;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_RECV_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_SEND_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_TIMEOUT;
import static com.ibm.stocator.fs.cos.COSConstants.USER_AGENT_PREFIX;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_USER_AGENT_PREFIX;
import static com.ibm.stocator.fs.cos.COSConstants.ENABLE_MULTI_DELETE;
import static com.ibm.stocator.fs.cos.COSConstants.PURGE_EXISTING_MULTIPART;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_PURGE_EXISTING_MULTIPART;
import static com.ibm.stocator.fs.cos.COSConstants.PURGE_EXISTING_MULTIPART_AGE;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_PURGE_EXISTING_MULTIPART_AGE;
import static com.ibm.stocator.fs.cos.COSConstants.FAST_UPLOAD;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_FAST_UPLOAD;
import static com.ibm.stocator.fs.cos.COSConstants.FAST_UPLOAD_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_FAST_UPLOAD_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.FAST_UPLOAD_ACTIVE_BLOCKS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS;
import static com.ibm.stocator.fs.cos.COSConstants.MAX_PAGING_KEYS;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_MAX_PAGING_KEYS;
import static com.ibm.stocator.fs.cos.COSConstants.FLAT_LISTING;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_FLAT_LISTING;
import static com.ibm.stocator.fs.cos.COSConstants.READAHEAD_RANGE;
import static com.ibm.stocator.fs.cos.COSConstants.DEFAULT_READAHEAD_RANGE;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADVISE;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADV_NORMAL;
import static com.ibm.stocator.fs.cos.COSConstants.BUFFER_DIR;
import static com.ibm.stocator.fs.common.Constants.FS_STOCATOR_FMODE_DATA_CLEANUP_DEFAULT;

import static com.ibm.stocator.fs.cos.COSUtils.translateException;

public class COSAPIClient implements IStoreClient {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(COSAPIClient.class);
  private static final String[] FS_ALT_KEYS = new String[]{FS_S3_A, FS_S3_D};
  /*
   * root bucket
   */
  private String mBucket;
  /*
   * COS client
   */
  private AmazonS3 mClient;

  /*
   * block size
   */
  private long mBlockSize;
  /*
   * If true, automatic delete will be activated on the data generated from
   * failed tasks
   */
  private boolean fModeAutomaticDelete;
  /*
   * If true, V2 signer will be created for authentication otherwise V4
   */
  private boolean mIsV2Signer;
  /*
   * support for different schema models
   */
  private String schemaProvided;

  /*
   * Contains map of objects that were created by successful jobs. Used in
   * container listing
   */
  private Map<String, Boolean> mCachedSparkJobsStatus;

  /*
   * Contains map of object names that were created. Used in container
   * listing
   */
  private Map<String, Boolean> mCachedSparkOriginated;

  private URI filesystemURI;
  private Configuration conf;
  private TransferManager transfers;
  private long partSize;
  private long multiPartThreshold;
  private ListeningExecutorService threadPoolExecutor;
  private ExecutorService unboundedThreadPool;
  private COSLocalDirAllocator directoryAllocator;
  private Path workingDir;
  private OnetimeInitialization singletoneInitTimeData;
  private boolean enableMultiObjectsDelete;
  private boolean blockUploadEnabled;
  private String blockOutputBuffer;
  private COSDataBlocks.BlockFactory blockFactory;
  private int blockOutputActiveBlocks;
  private MemoryCache memoryCache;
  private int maxKeys;
  private boolean flatListingFlag;
  private long readAhead;
  private COSInputPolicy inputPolicy;
  private int cacheSize;
  private Statistics statistics;
  private String bufferDirectory;
  private String bufferDirectoryKey;

  private final String amazonDefaultEndpoint = "s3.amazonaws.com";

  private StocatorPath stocatorPath;

  public COSAPIClient(URI pFilesystemURI, Configuration pConf) throws IOException {
    filesystemURI = pFilesystemURI;
    conf = pConf;
    LOG.info("Init :  {}", filesystemURI.toString());
    singletoneInitTimeData = OnetimeInitialization.getInstance();
  }

  @Override
  public void initiate(String scheme) throws IOException, ConfigurationParseException {
    mCachedSparkOriginated = new HashMap<String, Boolean>();
    mCachedSparkJobsStatus = new HashMap<String, Boolean>();
    schemaProvided = scheme;
    Properties props = ConfigurationHandler.initialize(filesystemURI, conf, scheme);
    // Set bucket name property
    int cacheSize = conf.getInt(CACHE_SIZE, GUAVA_CACHE_SIZE_DEFAULT);
    memoryCache = MemoryCache.getInstance(cacheSize);
    mBucket = props.getProperty(COS_BUCKET_PROPERTY);
    workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(filesystemURI,
        getWorkingDirectory());
    LOG.trace("Working directory set to {}", workingDir);
    fModeAutomaticDelete = "true".equals(conf.get(FS_STOCATOR_FMODE_DATA_CLEANUP,
        FS_STOCATOR_FMODE_DATA_CLEANUP_DEFAULT));
    mIsV2Signer = "true".equals(props.getProperty(V2_SIGNER_TYPE_COS_PROPERTY, "false"));
    // Define COS client
    String accessKey = props.getProperty(ACCESS_KEY_COS_PROPERTY);
    String secretKey = props.getProperty(SECRET_KEY_COS_PROPERTY);

    if (accessKey == null) {
      throw new ConfigurationParseException("Access KEY is empty. Please provide valid access key");
    }
    if (secretKey == null) {
      throw new ConfigurationParseException("Secret KEY is empty. Please provide valid secret key");
    }

    BasicAWSCredentials creds =
        new BasicAWSCredentials(accessKey, secretKey);
    ClientConfiguration clientConf = new ClientConfiguration();

    int maxThreads = Utils.getInt(conf, FS_COS, FS_ALT_KEYS, MAX_THREADS,
        DEFAULT_MAX_THREADS);
    if (maxThreads < 2) {
      LOG.warn(MAX_THREADS + " must be at least 2: forcing to 2.");
      maxThreads = 2;
    }
    int totalTasks = Utils.getInt(conf, FS_COS, FS_ALT_KEYS, MAX_TOTAL_TASKS,
        DEFAULT_MAX_TOTAL_TASKS);
    long keepAliveTime = Utils.getLong(conf, FS_COS, FS_ALT_KEYS, KEEPALIVE_TIME,
        DEFAULT_KEEPALIVE_TIME);
    threadPoolExecutor = BlockingThreadPoolExecutorService.newInstance(
        maxThreads,
        maxThreads + totalTasks,
        keepAliveTime, TimeUnit.SECONDS,
        "s3a-transfer-shared");

    unboundedThreadPool = new ThreadPoolExecutor(
        maxThreads, Integer.MAX_VALUE,
        keepAliveTime, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>(),
        BlockingThreadPoolExecutorService.newDaemonThreadFactory(
            "s3a-transfer-unbounded"));

    boolean secureConnections = Utils.getBoolean(conf, FS_COS, FS_ALT_KEYS,
        SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS);
    clientConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);

    String proxyHost = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
        PROXY_HOST, "");
    int proxyPort = Utils.getInt(conf, FS_COS, FS_ALT_KEYS, PROXY_PORT, -1);
    if (!proxyHost.isEmpty()) {
      clientConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        clientConf.setProxyPort(proxyPort);
      } else {
        if (secureConnections) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          clientConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          clientConf.setProxyPort(80);
        }
      }
      String proxyUsername = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
          PROXY_USERNAME);
      String proxyPassword = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
          PROXY_PASSWORD);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME + " or "
            + PROXY_PASSWORD + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      clientConf.setProxyUsername(proxyUsername);
      clientConf.setProxyPassword(proxyPassword);
      clientConf.setProxyDomain(Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
          PROXY_DOMAIN));
      clientConf.setProxyWorkstation(Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
          PROXY_WORKSTATION));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using proxy server {}:{} as user {} on "
            + "domain {} as workstation {}", clientConf.getProxyHost(),
              clientConf.getProxyPort(), String.valueOf(clientConf.getProxyUsername()),
              clientConf.getProxyDomain(),
              clientConf.getProxyWorkstation());
      }
    } else if (proxyPort >= 0) {
      String msg = "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    initConnectionSettings(conf,clientConf);
    if (mIsV2Signer) {
      clientConf.withSignerOverride("S3SignerType");
    }
    mClient = new AmazonS3Client(creds, clientConf);

    final String serviceUrl = props.getProperty(ENDPOINT_URL_COS_PROPERTY);
    if (serviceUrl != null && !serviceUrl.equals(amazonDefaultEndpoint)) {
      mClient.setEndpoint(serviceUrl);
    }
    mClient.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());

    // Set block size property
    String mBlockSizeString = props.getProperty(BLOCK_SIZE_COS_PROPERTY, "128");
    mBlockSize = Long.valueOf(mBlockSizeString).longValue() * 1024 * 1024L;
    bufferDirectory = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
        BUFFER_DIR);
    bufferDirectoryKey = Utils.getConfigKey(conf, FS_COS, FS_ALT_KEYS,
        BUFFER_DIR);

    LOG.trace("Buffer directory is set to {} for the key {}", bufferDirectory,
        bufferDirectoryKey);
    boolean autoCreateBucket =
        "true".equalsIgnoreCase((props.getProperty(AUTO_BUCKET_CREATE_COS_PROPERTY, "false")));

    partSize = Utils.getLong(conf, FS_COS, FS_ALT_KEYS, MULTIPART_SIZE,
        DEFAULT_MULTIPART_SIZE);
    multiPartThreshold = Utils.getLong(conf, FS_COS, FS_ALT_KEYS,
        MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);
    readAhead = Utils.getLong(conf, FS_COS, FS_ALT_KEYS, READAHEAD_RANGE,
        DEFAULT_READAHEAD_RANGE);
    LOG.debug(READAHEAD_RANGE + ":" + readAhead);
    inputPolicy = COSInputPolicy.getPolicy(
        Utils.getTrimmed(conf,  FS_COS, FS_ALT_KEYS, INPUT_FADVISE, INPUT_FADV_NORMAL));

    initTransferManager();
    maxKeys = Utils.getInt(conf, FS_COS, FS_ALT_KEYS, MAX_PAGING_KEYS, DEFAULT_MAX_PAGING_KEYS);
    flatListingFlag = Utils.getBoolean(conf, FS_COS, FS_ALT_KEYS, FLAT_LISTING,
        DEFAULT_FLAT_LISTING);

    if (autoCreateBucket) {
      try {
        boolean bucketExist = mClient.doesBucketExist(mBucket);
        if (bucketExist) {
          LOG.trace("Bucket {} exists", mBucket);
        } else {
          LOG.trace("Bucket {} doesn`t exists and autocreate", mBucket);
          String mRegion = props.getProperty(REGION_COS_PROPERTY);
          if (mRegion == null) {
            mClient.createBucket(mBucket);
          } else {
            LOG.trace("Creating bucket {} in region {}", mBucket, mRegion);
            mClient.createBucket(mBucket, mRegion);
          }
        }
      } catch (AmazonServiceException ase) {
        /*
        *  we ignore the BucketAlreadyExists exception since multiple processes or threads
        *  might try to create the bucket in parrallel, therefore it is expected that
        *  some will fail to create the bucket
        */
        if (!ase.getErrorCode().equals("BucketAlreadyExists")) {
          LOG.error(ase.getMessage());
          throw (ase);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage());
        throw (e);
      }
    }

    initMultipartUploads(conf);
    enableMultiObjectsDelete = Utils.getBoolean(conf, FS_COS, FS_ALT_KEYS,
        ENABLE_MULTI_DELETE, true);

    blockUploadEnabled = Utils.getBoolean(conf, FS_COS, FS_ALT_KEYS,
        FAST_UPLOAD, DEFAULT_FAST_UPLOAD);

    if (blockUploadEnabled) {
      blockOutputBuffer = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS, FAST_UPLOAD_BUFFER,
          DEFAULT_FAST_UPLOAD_BUFFER);
      partSize = COSUtils.ensureOutputParameterInRange(MULTIPART_SIZE, partSize);
      blockFactory = COSDataBlocks.createFactory(this, blockOutputBuffer);
      blockOutputActiveBlocks = Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
          FAST_UPLOAD_ACTIVE_BLOCKS, DEFAULT_FAST_UPLOAD_ACTIVE_BLOCKS);
      LOG.debug("Using COSBlockOutputStream with buffer = {}; block={};"
          + " queue limit={}",
          blockOutputBuffer, partSize, blockOutputActiveBlocks);
    } else {
      LOG.debug("Using COSOutputStream");
    }
  }

  @Override
  public long getBlockSize() {
    return mBlockSize;
  }

  @Override
  public String getDataRoot() {
    return mBucket;
  }

  /**
   * Request object metadata, Used to call _SUCCESS object or identify if objects
   * were generated by Stocator
   *
   * @param key key
   * @return the metadata
   */
  protected ObjectMetadata getObjectMetadata(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucket, key);
      return meta;
    } catch (AmazonClientException e) {
      LOG.debug(e.getMessage());
      return null;
    }
  }

  @Override
  public FileStatus getFileStatus(String hostName,
      Path path, String msg) throws IOException, FileNotFoundException {
    FileStatus res = null;
    FileStatus cached = memoryCache.getFileStatus(path.toString());
    if (cached != null) {
      return cached;
    }
    LOG.trace("getFileStatus(start) for {}, hostname: {}", path, hostName);
    /*
     * The requested path is equal to hostName. HostName is equal to
     * hostNameScheme, thus the container. Therefore we have no object to look
     * for and we return the FileStatus as a directory. Containers have to
     * lastModified.
     */
    if (path.toString().equals(hostName) || (path.toString().length() + 1 == hostName.length())) {
      LOG.trace("getFileStatus(completed) {}", path);
      res = new FileStatus(0L, true, 1, mBlockSize, 0L, path);
      memoryCache.putFileStatus(path.toString(), res);
      return res;
    }
    if (path.toString().contains(HADOOP_TEMPORARY)) {
      LOG.debug("getFileStatus on temp object {}. Return not found", path.toString());
      throw new FileNotFoundException("Not found " + path.toString());
    }
    String key = pathToKey(path);
    LOG.debug("getFileStatus: on original key {}", key);
    FileStatus fileStatus = null;
    try {
      fileStatus = getFileStatusKeyBased(key, path);
    } catch (AmazonS3Exception e) {
      LOG.warn("file status {} returned {}",
          key, e.getStatusCode());
      if (e.getStatusCode() != 404) {
        LOG.warn("Throw IOException for {}. Most likely authentication failed", key);
        throw new IOException(e);
      }
    }
    if (fileStatus != null) {
      LOG.trace("getFileStatus(completed) {}", path);
      memoryCache.putFileStatus(path.toString(), fileStatus);
      return fileStatus;
    }
    // means key returned not found. Trying to call get file status on key/
    // probably not needed this call
    if (!key.endsWith("/")) {
      String newKey = key + "/";
      try {
        LOG.debug("getFileStatus: original key not found. Alternative key {}", newKey);
        fileStatus = getFileStatusKeyBased(newKey, path);
      } catch (AmazonS3Exception e) {
        if (e.getStatusCode() != 404) {
          throw new IOException(e);
        }
      }

      if (fileStatus != null) {
        LOG.trace("getFileStatus(completed) {}", path);
        memoryCache.putFileStatus(path.toString(), fileStatus);
        return fileStatus;
      } else {
        // if here: both key and key/ returned not found.
        // trying to see if pseudo directory of the form
        // a/b/key/d/e (a/b/key/ doesn't exists by itself)
        // perform listing on the key
        LOG.debug("getFileStatus: Modifined key {} not found. Trying to list", key);
        key = maybeAddTrailingSlash(key);
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(mBucket);
        request.setPrefix(key);
        request.withEncodingType("UTF-8");
        request.setDelimiter("/");
        request.setMaxKeys(1);

        ObjectListing objects = mClient.listObjects(request);
        if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjectSummaries().isEmpty()) {
          LOG.debug("getFileStatus(completed) {}", path);
          res = new FileStatus(0, true, 1, 0, 0, path);
          memoryCache.putFileStatus(path.toString(), res);
          return res;
        } else if (key.isEmpty()) {
          LOG.trace("Found root directory");
          LOG.debug("getFileStatus(completed) {}", path);
          res = new FileStatus(0, true, 1, 0, 0, path);
          memoryCache.putFileStatus(path.toString(), res);
          return res;
        }
      }
    }
    LOG.debug("Not found {}. Throw FNF exception", path.toString());
    throw new FileNotFoundException("Not found " + path.toString());
  }

  private FileStatus getFileStatusKeyBased(String key, Path path) throws AmazonS3Exception {
    LOG.trace("internal method - get file status by key {}, path {}", key, path);
    FileStatus cachedFS = memoryCache.getFileStatus(path.toString());
    if (cachedFS != null) {
      return cachedFS;
    }
    ObjectMetadata meta = mClient.getObjectMetadata(mBucket, key);
    String sparkOrigin = meta.getUserMetaDataOf("data-origin");
    boolean stocatorCreated = false;
    if (sparkOrigin != null) {
      String tmp = (String) sparkOrigin;
      if (tmp.equals("stocator")) {
        stocatorCreated = true;
      }
    }
    mCachedSparkOriginated.put(key, Boolean.valueOf(stocatorCreated));
    FileStatus fs = createFileStatus(meta.getContentLength(), key, meta.getLastModified(), path);
    LOG.trace("getFileStatusKeyBased: key {} fs.path {}", key, fs.getPath());
    memoryCache.putFileStatus(path.toString(), fs);
    return fs;
  }

  private FileStatus createFileStatus(S3ObjectSummary objSummary,
      String hostName, Path path)
      throws IllegalArgumentException, IOException {
    String objKey = objSummary.getKey();
    String newMergedPath = getMergedPath(hostName, path, objKey);
    return createFileStatus(objSummary.getSize(), objKey,
        objSummary.getLastModified(), new Path(newMergedPath));
  }

  private FileStatus createFileStatus(long contentlength, String key,
      Date lastModified, Path path) {
    if (objectRepresentsDirectory(key, contentlength)) {
      LOG.debug("Found exact file: fake directory {}", path.toString());
      return new FileStatus(0, true, 1, 0, 0, path);
    } else {
      LOG.debug("Found exact file: normal file {}", path.toString());
      long fileModificationTime = 0L;
      if (lastModified != null) {
        fileModificationTime = lastModified.getTime();
      }
      return new FileStatus(contentlength, false, 1, mBlockSize, fileModificationTime, path);
    }
  }

  /**
   * Turns a path (relative or otherwise) into an COS key, adding a trailing "/"
   * if the path is not the root <i>and</i> does not already have a "/" at the
   * end.
   *
   * @param key COS key or ""
   * @return the with a trailing "/", or, if it is the root key, "",
   */
  private String maybeAddTrailingSlash(String key) {
    if (!key.isEmpty() && !key.endsWith("/")) {
      return key + '/';
    } else {
      return key;
    }
  }

  /**
   * Predicate: does the object represent a directory?
   *
   * @param name object name
   * @param size object size
   * @return true if it meets the criteria for being an object
   */
  public static boolean objectRepresentsDirectory(final String name, final long size) {
    return !name.isEmpty() && size == 0L;
  }

  @Override
  public boolean exists(String hostName, Path path) throws IOException, FileNotFoundException {
    LOG.trace("Object exists: {}", path);
    String objName = pathToKey(path);
    if (objName.contains(HADOOP_TEMPORARY)) {
      LOG.debug("Exists on temp object {}. Return false", objName);
      return false;
    }
    try {
      if (getFileStatus(hostName, path, "exists") != null) {
        return true;
      }
    } catch (FileNotFoundException e) {
      return false;
    }
    return false;
  }

  @Override
  public FSDataInputStream getObject(String hostName, Path path) throws IOException {
    LOG.debug("Opening '{}' for reading.", path);
    String key = pathToKey(path);
    FileStatus fileStatus = memoryCache.getFileStatus(path.toString());
    if (fileStatus == null) {
      fileStatus = getFileStatus(hostName, path, "getObject");
    }
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException("Can't open " + path
          + " because it is a directory");
    }
    COSInputStream inputStream = new COSInputStream(mBucket, key,
        fileStatus.getLen(), mClient, readAhead, inputPolicy, statistics);

    return new FSDataInputStream(inputStream);
  }

  @Override
  public FSDataOutputStream createObject(String objName, String contentType,
      Map<String, String> metadata,
      Statistics statistics) throws IOException {
    LOG.debug("Create object {}", objName);
    try {
      String objNameWithoutBuket = objName;
      if (objName.startsWith(mBucket + "/")) {
        objNameWithoutBuket = objName.substring(mBucket.length() + 1);
      }
      if (blockUploadEnabled) {
        return new FSDataOutputStream(
            new COSBlockOutputStream(this,
                objNameWithoutBuket,
                new SemaphoredDelegatingExecutor(threadPoolExecutor,
                    blockOutputActiveBlocks, true),
                partSize,
                blockFactory,
                contentType,
                new WriteOperationHelper(objNameWithoutBuket),
                metadata
            ),
            null);
      }

      if (!contentType.equals(Constants.APPLICATION_DIRECTORY)) {
        return new FSDataOutputStream(new COSOutputStream(mBucket, objName,
            mClient, contentType, metadata, transfers, this), statistics);
      } else {
        final InputStream im = new InputStream() {
          @Override
          public int read() throws IOException {
            return -1;
          }
        };
        final ObjectMetadata om = new ObjectMetadata();
        om.setContentLength(0L);
        om.setContentType(contentType);
        om.setUserMetadata(metadata);
        // Remove the bucket name prefix from key path
        if (objName.startsWith(mBucket + "/")) {
          objName = objName.substring(mBucket.length() + 1);
        }
        /*
        if (!objName.endsWith("/")) {
          objName = objName + "/";
        }*/
        LOG.debug("bucket: {}, key {}", mBucket, objName);
        PutObjectRequest putObjectRequest = new PutObjectRequest(mBucket, objName, im, om);
        Upload upload = transfers.upload(putObjectRequest);
        upload.waitForUploadResult();
        OutputStream fakeStream = new OutputStream() {

          @Override
          public void write(int b) throws IOException {
          }

          @Override
          public void close() throws IOException {
            super.close();
          }
        };
        return new FSDataOutputStream(fakeStream, statistics);
      }
    } catch (InterruptedException e) {
      throw new InterruptedIOException("Interrupted creating " + objName);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  /**
   * Create a putObject request.
   * Adds the ACL and metadata
   * @param key key of object
   * @param metadata metadata header
   * @param srcfile source file
   * @return the request
   */
  public PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, File srcfile) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(mBucket, key,
        srcfile);
    putObjectRequest.setMetadata(metadata);
    return putObjectRequest;
  }

  /**
   * Create a {@link PutObjectRequest} request.
   * The metadata is assumed to have been configured with the size of the
   * operation.
   * @param key key of object
   * @param metadata metadata header
   * @param inputStream source data
   * @return the request
   */
  private PutObjectRequest newPutObjectRequest(String key,
      ObjectMetadata metadata, InputStream inputStream) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(mBucket, key,
        inputStream, metadata);
    return putObjectRequest;
  }

  public void setStocatorPath(StocatorPath sp) {
    stocatorPath = sp;
  }

  @Override
  public String getScheme() {
    return schemaProvided;
  }

  @Override
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException {
    String key = pathToKey(path);
    LOG.debug("Object name to delete {}. Path {}", key, path.toString());
    try {
      mClient.deleteObject(new DeleteObjectRequest(mBucket, key));
      memoryCache.removeFileStatus(path.toString());
      return true;
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() != 404) {
        throw new IOException(e);
      }
    }
    LOG.warn("Delete on {} not found. Nothing to delete");
    return false;
  }

  public URI getAccessURI() throws IOException {
    return filesystemURI;
  }

  /**
   * Set the current working directory for the given file system. All relative
   * paths will be resolved relative to it.
   *
   * @param newDir new directory
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public FileStatus[] list(String hostName, Path path, boolean fullListing,
      boolean prefixBased, Boolean isDirectory,
      boolean flatListing, PathFilter filter) throws FileNotFoundException, IOException {
    return internalList(hostName, path, fullListing, prefixBased, isDirectory, flatListing,
        filter, fModeAutomaticDelete);

  }

  private FileStatus[] internalList(String hostName, Path path, boolean fullListing,
      boolean prefixBased, Boolean isDirectory,
      boolean flatListing, PathFilter filter,
      boolean cleanup) throws FileNotFoundException, IOException {
    LOG.debug("list:(start) {}. full listing {}, prefix based {}, flat list {}",
        path, fullListing, prefixBased, flatListing);
    ArrayList<FileStatus> tmpResult = new ArrayList<FileStatus>();
    String key = pathToKey(path);
    if (isDirectory != null && isDirectory.booleanValue() && !key.endsWith("/")
        && !path.toString().equals(hostName)) {
      key = key + "/";
      LOG.debug("list:(mid) {}, modify key to {}", path, key);
    }

    Map<String, FileStatus> emptyObjects = new HashMap<String, FileStatus>();
    ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(mBucket);
    request.setMaxKeys(5000);
    request.setPrefix(key);
    request.withEncodingType("UTF-8");
    if (!flatListing) {
      LOG.trace("list:(mid) {}, set delimiter", path);
      request.setDelimiter("/");
    }

    ObjectListing objectList = mClient.listObjects(request);

    List<S3ObjectSummary> objectSummaries = objectList.getObjectSummaries();
    List<String> commonPrefixes = objectList.getCommonPrefixes();

    boolean objectScanContinue = true;
    S3ObjectSummary prevObj = null;
    // start FTA logic
    boolean stocatorOrigin = isSparkOrigin(key);
    if (stocatorOrigin) {
      LOG.debug("Stocator origin is true for {}", key);
      if (!isJobSuccessful(key)) {
        LOG.warn("{} created by failed Spark job. Skipped. Delete temporarily disabled ", key);
        /*
        if (cleanup) {
          delete(hostName, new Path(key), true);
        }
        */
        return new FileStatus[0];
      }
    }
    while (objectScanContinue) {
      for (S3ObjectSummary obj : objectSummaries) {
        if (prevObj == null) {
          prevObj = obj;
          prevObj.setKey(correctPlusSign(key, prevObj.getKey()));
          continue;
        }
        obj.setKey(correctPlusSign(key, obj.getKey()));
        String objKey = obj.getKey();
        String unifiedObjectName = stocatorPath.extractUnifiedName(objKey);
        LOG.trace("list candidate {}, unified name {}", objKey, unifiedObjectName);
        stocatorOrigin = isSparkOrigin(unifiedObjectName);
        if (stocatorOrigin && !fullListing) {
          if (!isJobSuccessful(unifiedObjectName)) {
            // a bit tricky. need to delete entire set
            // having unified name as a prefix
            continue;
          }
          LOG.trace("{} created by Spark", unifiedObjectName);
          // if we here - data created by spark and job completed
          // successfully
          // however there be might parts of failed tasks that
          // were not aborted
          // we need to make sure there are no failed attempts
          if (stocatorPath.nameWithoutTaskID(objKey)
              .equals(stocatorPath.nameWithoutTaskID(prevObj.getKey()))) {
            // found failed that was not aborted.
            LOG.trace("Colisiion found between {} and {}", prevObj.getKey(), objKey);
            if (prevObj.getSize() < obj.getSize()) {
              LOG.trace("New candidate is {}. Removed {}", obj.getKey(), prevObj.getKey());
              if (cleanup) {
                String newMergedPath = getMergedPath(hostName, path, prevObj.getKey());
                LOG.warn("Delete failed data part {}", newMergedPath);
                delete(hostName, new Path(newMergedPath) , true);
              }
              prevObj = obj;
            } else {
              if (cleanup) {
                String newMergedPath = getMergedPath(hostName, path, obj.getKey());
                LOG.warn("Delete failed data part {}", newMergedPath);
                delete(hostName, new Path(newMergedPath) , true);
              }
            }
            continue;
          }
        }
        FileStatus fs = createFileStatus(prevObj, hostName, path);
        if (fs.getLen() > 0 || fullListing) {
          LOG.trace("Native direct list. Adding {} size {}",fs.getPath(), fs.getLen());
          if (filter == null) {
            tmpResult.add(fs);
          } else if (filter != null && filter.accept(fs.getPath())) {
            tmpResult.add(fs);
          } else {
            LOG.trace("{} rejected by path filter during list. Filter {}",
                fs.getPath(), filter);
          }
        } else {
          emptyObjects.put(fs.getPath().toString(), fs);
        }
        prevObj = obj;
      }
      boolean isTruncated = objectList.isTruncated();
      if (isTruncated) {
        objectList = mClient.listNextBatchOfObjects(objectList);
        objectSummaries = objectList.getObjectSummaries();
      } else {
        objectScanContinue = false;
      }
    }

    if (prevObj != null) {
      FileStatus fs = createFileStatus(prevObj, hostName, path);
      LOG.trace("Adding the last object from the list {}", fs.getPath());
      if (fs.getLen() > 0 || fullListing) {
        LOG.trace("Native direct list. Adding {} size {}",fs.getPath(), fs.getLen());
        if (filter == null) {
          memoryCache.putFileStatus(fs.getPath().toString(), fs);
          tmpResult.add(fs);
        } else if (filter != null && filter.accept(fs.getPath())) {
          memoryCache.putFileStatus(fs.getPath().toString(), fs);
          tmpResult.add(fs);
        } else {
          LOG.trace("{} rejected by path filter during list. Filter {}",
              fs.getPath(), filter);
        }
      } else if (!fs.getPath().getName().equals(HADOOP_SUCCESS)) {
        emptyObjects.put(fs.getPath().toString(), fs);
      }
    }

    // get common prefixes
    for (String comPrefix : commonPrefixes) {
      LOG.trace("Common prefix is {}", comPrefix);
      if (emptyObjects.containsKey(keyToQualifiedPath(hostName,
          comPrefix).toString()) || emptyObjects.isEmpty()) {
        FileStatus status = new COSFileStatus(true, false, keyToQualifiedPath(hostName,
            comPrefix));
        LOG.trace("Match between common prefix and empty object {}. Adding to result", comPrefix);
        if (filter == null) {
          memoryCache.putFileStatus(status.getPath().toString(), status);
          tmpResult.add(status);
        } else if (filter != null && filter.accept(status.getPath())) {
          memoryCache.putFileStatus(status.getPath().toString(), status);
          tmpResult.add(status);
        } else {
          LOG.trace("Common prefix {} rejected by path filter during list. Filter {}",
              status.getPath(), filter);
        }
      }
    }
    return tmpResult.toArray(new FileStatus[tmpResult.size()]);
  }

  /**
   * Merge between two paths
   *
   * @param hostName
   * @param p path
   * @param objectKey
   * @return merged path
   */
  private String getMergedPath(String hostName, Path p, String objectKey) {
    if ((p.getParent() != null) && (p.getName() != null)
        && (p.getParent().toString().equals(hostName))) {
      if (objectKey.equals(p.getName())) {
        return p.toString();
      }
      return hostName + objectKey;
    }
    return hostName + objectKey;
  }

  /**
   * Turns a path (relative or otherwise) into an COS key
   *
   * @param path object full path
   */

  private String pathToKey(Path path) {
    if (!path.isAbsolute()) {
      path = new Path(workingDir, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  public Path keyToQualifiedPath(String hostName, String key) {
    return new Path(hostName, key);
  }

  /**
   * Checks if container/object contains container/object/_SUCCESS If so, this
   * object was created by successful Hadoop job
   *
   * @param objectKey
   * @return boolean if job is successful
   */
  private boolean isJobSuccessful(String objectKey) {
    LOG.trace("isJobSuccessful: for {}", objectKey);
    if (mCachedSparkJobsStatus.containsKey(objectKey)) {
      LOG.trace("isJobSuccessful: {} found cached", objectKey);
      return mCachedSparkJobsStatus.get(objectKey).booleanValue();
    }
    String key = getRealKey(objectKey);
    Path p = new Path(key, HADOOP_SUCCESS);
    ObjectMetadata statusMetadata = getObjectMetadata(p.toString());
    Boolean isJobOK = Boolean.FALSE;
    if (statusMetadata != null) {
      isJobOK = Boolean.TRUE;
    }
    LOG.debug("isJobSuccessful: not cached {}. Status is {}", objectKey, isJobOK);
    mCachedSparkJobsStatus.put(objectKey, isJobOK);
    return isJobOK.booleanValue();
  }

  /**
   * Checks if container/object exists and verifies that it contains
   * Data-Origin=stocator metadata If so, object was created by Spark.
   *
   * @param objectKey the key of the object
   * @return boolean if object was created by Spark
   */
  private boolean isSparkOrigin(String objectKey) {
    LOG.debug("check spark origin for {}", objectKey);
    if (!objectKey.endsWith("/")) {
      LOG.debug("Key {} has no slash. Return false", objectKey);
      return false;
    } else {
      objectKey = objectKey.substring(0, objectKey.length() - 1);
    }

    if (mCachedSparkOriginated.containsKey(objectKey)) {
      boolean res = mCachedSparkOriginated.get(objectKey).booleanValue();
      LOG.debug("found cached for spark origin for {}. Status {}", objectKey, res);
      return res;
    }
    String key = getRealKey(objectKey);
    Boolean sparkOriginated = Boolean.FALSE;
    ObjectMetadata objMetadata = getObjectMetadata(key);
    if (objMetadata != null) {
      Object sparkOrigin = objMetadata.getUserMetaDataOf("data-origin");
      if (sparkOrigin != null) {
        String tmp = (String) sparkOrigin;
        if (tmp.equals("stocator")) {
          sparkOriginated = Boolean.TRUE;
        }
      }
    }
    mCachedSparkOriginated.put(key, sparkOriginated);
    LOG.debug("spark origin for {} is {} non cached", objectKey, sparkOriginated.booleanValue());
    return sparkOriginated.booleanValue();
  }

  private String getRealKey(String objectKey) {
    String key = objectKey;
    if (objectKey.toString().startsWith(mBucket + "/")
        && !objectKey.toString().equals(mBucket + "/")) {
      key = objectKey.substring(mBucket.length() + 1);
    }
    return key;
  }

  private void initMultipartUploads(Configuration conf) throws IOException {
    boolean purgeExistingMultipart = Utils.getBoolean(conf, FS_COS, FS_ALT_KEYS,
        PURGE_EXISTING_MULTIPART,
        DEFAULT_PURGE_EXISTING_MULTIPART);
    long purgeExistingMultipartAge = Utils.getLong(conf, FS_COS, FS_ALT_KEYS,
        PURGE_EXISTING_MULTIPART_AGE, DEFAULT_PURGE_EXISTING_MULTIPART_AGE);

    if (purgeExistingMultipart) {
      Date purgeBefore =
          new Date(new Date().getTime() - purgeExistingMultipartAge * 1000);

      try {
        transfers.abortMultipartUploads(mBucket, purgeBefore);
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() == 403) {
          LOG.debug("Failed to purging multipart uploads against {},"
              + " FS may be read only", mBucket, e);
        } else {
          throw translateException("purging multipart uploads", mBucket, e);
        }
      }
    }
  }

  /**
   * Initializes connection management
   *
   * @param conf Hadoop configuration
   * @param clientConf client SDK configuration
   */
  private void initConnectionSettings(Configuration conf,
      ClientConfiguration clientConf) throws IOException {
    clientConf.setMaxConnections(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        MAXIMUM_CONNECTIONS, DEFAULT_MAXIMUM_CONNECTIONS));
    clientConf.setClientExecutionTimeout(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        CLIENT_EXEC_TIMEOUT, DEFAULT_CLIENT_EXEC_TIMEOUT));
    clientConf.setMaxErrorRetry(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        MAX_ERROR_RETRIES, DEFAULT_MAX_ERROR_RETRIES));
    clientConf.setConnectionTimeout(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        ESTABLISH_TIMEOUT, DEFAULT_ESTABLISH_TIMEOUT));
    clientConf.setSocketTimeout(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT));
    clientConf.setRequestTimeout(Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT));
    int sockSendBuffer = Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        SOCKET_SEND_BUFFER, DEFAULT_SOCKET_SEND_BUFFER);
    int sockRecvBuffer = Utils.getInt(conf, FS_COS, FS_ALT_KEYS,
        SOCKET_RECV_BUFFER, DEFAULT_SOCKET_RECV_BUFFER);
    clientConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
    String signerOverride = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
        SIGNING_ALGORITHM, "");
    if (!signerOverride.isEmpty()) {
      LOG.debug("Signer override = {}", signerOverride);
      clientConf.setSignerOverride(signerOverride);
    }

    String userAgentPrefix = Utils.getTrimmed(conf, FS_COS, FS_ALT_KEYS,
        USER_AGENT_PREFIX, DEFAULT_USER_AGENT_PREFIX);
    String userAgentName = singletoneInitTimeData.getUserAgentName();
    if (!userAgentPrefix.equals(DEFAULT_USER_AGENT_PREFIX)) {
      userAgentName = userAgentPrefix + " " + userAgentName;
    }
    clientConf.setUserAgentPrefix(userAgentName);
  }

  @Override
  public boolean rename(String hostName, String srcPath, String dstPath) throws IOException {
    // Not yet implemented
    return false;
  }

  private void initTransferManager() {
    TransferManagerConfiguration transferConfiguration =
        new TransferManagerConfiguration();
    transferConfiguration.setMinimumUploadPartSize(partSize);
    transferConfiguration.setMultipartUploadThreshold(multiPartThreshold);
    transferConfiguration.setMultipartCopyPartSize(partSize);
    transferConfiguration.setMultipartCopyThreshold(multiPartThreshold);

    transfers = new TransferManager(mClient, unboundedThreadPool);
    transfers.setConfiguration(transferConfiguration);
  }

  public synchronized File createTmpFileForWrite(String pathStr) throws IOException {
    LOG.trace("createTmpFileForWrite {}", pathStr);
    if (directoryAllocator == null) {
      String bufferDirKey = bufferDirectory != null
          ? bufferDirectoryKey : "hadoop.tmp.dir";
      LOG.trace("Local buffer directorykey is {}", bufferDirKey);
      directoryAllocator = new COSLocalDirAllocator(bufferDirKey);
    }
    return directoryAllocator.createTmpFileForWrite(pathStr,
      COSLocalDirAllocator.SIZE_UNKNOWN, conf);
  }

  /**
   * Upload part of a multi-partition file.
   * <i>Important: this call does not close any input stream in the request.</i>
   * @param request request
   * @return the result of the operation
   * @throws AmazonClientException on problems
   */
  public UploadPartResult uploadPart(UploadPartRequest request)
      throws AmazonClientException {
    try {
      UploadPartResult uploadPartResult = mClient.uploadPart(request);
      return uploadPartResult;
    } catch (AmazonClientException e) {
      throw e;
    }
  }

  final class WriteOperationHelper {
    private final String key;

    private WriteOperationHelper(String keyT) {
      key = keyT;
    }

    /**
     * Create a {@link PutObjectRequest} request.
     * If {@code length} is set, the metadata is configured with the size of
     * the upload.
     * @param inputStream source data
     * @param length size, if known. Use -1 for not known
     * @return the request
     */
    PutObjectRequest newPutRequest(InputStream inputStream, long length) {
      PutObjectRequest request = newPutObjectRequest(key,
          newObjectMetadata(length), inputStream);
      return request;
    }

    /**
     * Create a {@link PutObjectRequest} request to upload a file.
     * @param sourceFile source file
     * @return the request
     */
    PutObjectRequest newPutRequest(File sourceFile) {
      int length = (int) sourceFile.length();
      PutObjectRequest request = newPutObjectRequest(key,
          newObjectMetadata(length), sourceFile);
      return request;
    }

    /**
     * Callback on a successful write.
     */
    void writeSuccessful() {
      LOG.debug("successful write");
    }

    /**
     * A helper method to delete a list of keys on a s3-backend.
     *
     * @param keysToDelete collection of keys to delete on the s3-backend
     *        if empty, no request is made of the object store.
     * @param clearKeys clears the keysToDelete-list after processing the list
     *            when set to true
     * @param deleteFakeDir indicates whether this is for deleting fake dirs
     * @throws InvalidRequestException if the request was rejected due to
     * a mistaken attempt to delete the root directory
     */
    private void removeKeys(List<DeleteObjectsRequest.KeyVersion> keysToDelete,
        boolean clearKeys, boolean deleteFakeDir)
        throws AmazonClientException, InvalidRequestException {
      if (keysToDelete.isEmpty()) {
        // exit fast if there are no keys to delete
        return;
      }
      for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
        blockRootDelete(keyVersion.getKey());
      }
      if (enableMultiObjectsDelete) {
        mClient.deleteObjects(new DeleteObjectsRequest(mBucket).withKeys(keysToDelete));
      } else {
        for (DeleteObjectsRequest.KeyVersion keyVersion : keysToDelete) {
          String key = keyVersion.getKey();
          blockRootDelete(key);
          mClient.deleteObject(mBucket, key);
        }
      }
      if (clearKeys) {
        keysToDelete.clear();
      }
    }

    /**
     * Reject any request to delete an object where the key is root.
     * @param key key to validate
     * @throws InvalidRequestException if the request was rejected due to
     * a mistaken attempt to delete the root directory
     */
    private void blockRootDelete(String key) throws InvalidRequestException {
      if (key.isEmpty() || "/".equals(key)) {
        throw new InvalidRequestException("Bucket " + mBucket
            + " cannot be deleted");
      }
    }

    /**
     * Callback on a write failure.
     * @param e Any exception raised which triggered the failure
     */
    void writeFailed(Exception e) {
      LOG.debug("Write to {} failed", this, e);
    }

    /**
     * Create a new object metadata instance.
     * Any standard metadata headers are added here, for example:
     * encryption
     * @param length size, if known. Use -1 for not known
     * @return a new metadata instance
     */
    public ObjectMetadata newObjectMetadata(long length) {
      final ObjectMetadata om = new ObjectMetadata();
      if (length >= 0) {
        om.setContentLength(length);
      }
      return om;
    }

    /**
     * Start the multipart upload process.
     * @return the upload result containing the ID
     * @throws IOException IO problem
     */
    String initiateMultiPartUpload() throws IOException {
      LOG.debug("Initiating Multipart upload");
      final InitiateMultipartUploadRequest initiateMPURequest =
          new InitiateMultipartUploadRequest(mBucket,
              key,
              newObjectMetadata(-1));
      try {
        return mClient.initiateMultipartUpload(initiateMPURequest)
            .getUploadId();
      } catch (AmazonClientException ace) {
        throw translateException("initiate MultiPartUpload", key, ace);
      }
    }

    /**
     * Complete a multipart upload operation.
     * @param uploadId multipart operation Id
     * @param partETags list of partial uploads
     * @return the result
     * @throws AmazonClientException on problems
     */
    CompleteMultipartUploadResult completeMultipartUpload(String uploadId,
        List<PartETag> partETags) throws AmazonClientException {
      LOG.debug("Completing multipart upload {} with {} parts",
          uploadId, partETags.size());
      return mClient.completeMultipartUpload(
          new CompleteMultipartUploadRequest(mBucket,
              key,
              uploadId,
              partETags));
    }

    /**
     * Abort a multipart upload operation
     * @param uploadId multipart operation Id
     * @throws AmazonClientException on problems
     */
    void abortMultipartUpload(String uploadId) throws AmazonClientException {
      LOG.debug("Aborting multipart upload {}", uploadId);
      mClient.abortMultipartUpload(
          new AbortMultipartUploadRequest(mBucket, key, uploadId));
    }

    /**
     * Create and initialize a part request of a multipart upload.
     * Exactly one of: {@code uploadStream} or {@code sourceFile}
     * must be specified.
     * @param uploadId ID of ongoing upload
     * @param partNumber current part number of the upload
     * @param size amount of data
     * @param uploadStream source of data to upload
     * @param sourceFile optional source file
     * @return the request
     */
    UploadPartRequest newUploadPartRequest(String uploadId,
        int partNumber, int size, InputStream uploadStream, File sourceFile) {
      Preconditions.checkNotNull(uploadId);
      // exactly one source must be set; xor verifies this
      Preconditions.checkArgument((uploadStream != null) ^ (sourceFile != null),
          "Data source");
      Preconditions.checkArgument(size > 0, "Invalid partition size %s", size);
      Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000,
          "partNumber must be between 1 and 10000 inclusive, but is %s",
          partNumber);

      LOG.debug("Creating part upload request for {} #{} size {}",
          uploadId, partNumber, size);
      UploadPartRequest request = new UploadPartRequest()
          .withBucketName(mBucket)
          .withKey(key)
          .withUploadId(uploadId)
          .withPartNumber(partNumber)
          .withPartSize(size);
      if (uploadStream != null) {
        // there's an upload stream. Bind to it.
        request.setInputStream(uploadStream);
      } else {
        request.setFile(sourceFile);
      }
      return request;
    }

    /**
     * The toString method is intended to be used in logging/toString calls.
     * @return a string description
     */
    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "{bucket=").append(mBucket);
      sb.append(", key='").append(key).append('\'');
      sb.append('}');
      return sb.toString();
    }

    /**
     * PUT an object directly (i.e. not via the transfer manager).
     * @param putObjectRequest the request
     * @return the upload initiated
     * @throws IOException on problems
     */
    PutObjectResult putObject(PutObjectRequest putObjectRequest)
        throws IOException {
      try {
        PutObjectResult result = mClient.putObject(putObjectRequest);
        return result;
      } catch (AmazonClientException e) {
        throw translateException("put", putObjectRequest.getKey(), e);
      }
    }
  }

  /**
   * Initiate a {@code listObjects} operation, incrementing metrics
   * in the process.
   * @param request request to initiate
   * @return the results
   */
  protected ObjectListing listObjects(ListObjectsRequest request) {
    return mClient.listObjects(request);
  }

  /**
   * List the next set of objects.
   * @param objects paged result
   * @return the next result object
   */
  protected ObjectListing continueListObjects(ObjectListing objects) {
    return mClient.listNextBatchOfObjects(objects);
  }

  /**
   * Get the maximum key count.
   * @return a value, valid after initialization
   */
  int getMaxKeys() {
    return maxKeys;
  }

  /**
   * Build a {@link LocatedFileStatus} from a {@link FileStatus} instance.
   * @param status file status
   * @return a located status with block locations set up from this FS
   * @throws IOException IO Problems
   */
  LocatedFileStatus toLocatedFileStatus(FileStatus status)
      throws IOException {
    return new LocatedFileStatus(status, null);
  }

  @Override
  public boolean isFlatListing() {
    return flatListingFlag;
  }

  @Override
  public void setStatistics(Statistics stat) {
    statistics = stat;
  }

  /**
   * Qualify a path
   * @param path path to qualify
   * @return a qualified path
   */

  public Path qualify(Path path) {
    return innerQualify(path);
  }

  private Path innerQualify(Path path) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Working directory {}", workingDir);
      Path p = path.makeQualified(filesystemURI, workingDir);
      LOG.trace("Qualify path from {} to {}", path, p);
      return p;
    }
    return path.makeQualified(filesystemURI, workingDir);
  }

  /**
   * Due to SDK bug, list operations may return strings that has spaces instead of +
   * This method will try to fix names for known patterns
   *
   * @param origin original string that may contain
   * @param stringToCorrect string that may contain original string with spaces instead of +
   * @return corrected string
   */
  private String correctPlusSign(String origin, String stringToCorrect) {
    if (origin.contains("+")) {
      LOG.debug("Adapt plus sign in {} to avoid SDK bug on {}", origin, stringToCorrect);
      StringBuilder tmpStringToCorrect = new StringBuilder(stringToCorrect);
      boolean hasSign = true;
      int fromIndex = 0;
      while (hasSign) {
        int plusLocation = origin.indexOf("+", fromIndex);
        if (plusLocation < 0) {
          hasSign = false;
          break;
        }
        if (tmpStringToCorrect.charAt(plusLocation) == ' ') {
          tmpStringToCorrect.setCharAt(plusLocation, '+');
        }
        if (origin.length() <= plusLocation + 1) {
          fromIndex = plusLocation + 1;
        } else {
          fromIndex = origin.length();
        }
      }
      LOG.debug("Adapt plus sign {} corrected to {}", stringToCorrect,
          tmpStringToCorrect.toString());
      return tmpStringToCorrect.toString();
    }
    return stringToCorrect;
  }
}
