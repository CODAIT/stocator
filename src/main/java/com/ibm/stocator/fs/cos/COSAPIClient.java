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
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;

import com.ibm.oauth.BasicIBMOAuthCredentials;
import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.StocatorPath;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.ConfigurationParseException;
import com.ibm.stocator.fs.cos.ConfigurationHandler;
import com.ibm.stocator.fs.cos.OnetimeInitialization;
import com.ibm.stocator.fs.cos.COSInputStream;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SDKGlobalConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.fs.Path;

import static com.ibm.stocator.fs.common.Constants.HADOOP_SUCCESS;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.cos.COSConstants.BUFFER_DIR;
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
import static com.ibm.stocator.fs.cos.COSConstants.API_KEY_IAM_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.IAM_ENDPOINT_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.IAM_SERVICE_INSTANCE_ID_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.SECRET_KEY_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.BLOCK_SIZE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.COS_BUCKET_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.ENDPOINT_URL_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.FMODE_AUTOMATIC_DELETE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.REGION_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.V2_SIGNER_TYPE_COS_PROPERTY;
import static com.ibm.stocator.fs.cos.COSConstants.SECURE_CONNECTIONS;
import static com.ibm.stocator.fs.cos.COSConstants.SIGNING_ALGORITHM;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_RECV_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_SEND_BUFFER;
import static com.ibm.stocator.fs.cos.COSConstants.SOCKET_TIMEOUT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;

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
  private LocalDirAllocator directoryAllocator;
  private Path workingDir;
  private OnetimeInitialization singletoneInitTimeData;

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
    mBucket = props.getProperty(COS_BUCKET_PROPERTY);
    workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(filesystemURI,
        getWorkingDirectory());

    fModeAutomaticDelete =
        "true".equals(props.getProperty(FMODE_AUTOMATIC_DELETE_COS_PROPERTY, "false"));
    mIsV2Signer = "true".equals(props.getProperty(V2_SIGNER_TYPE_COS_PROPERTY, "false"));
    // Define COS client
    String accessKey = props.getProperty(ACCESS_KEY_COS_PROPERTY);
    String secretKey = props.getProperty(SECRET_KEY_COS_PROPERTY);
    String apiKey = props.getProperty(API_KEY_IAM_PROPERTY);

    if (apiKey == null && accessKey == null) {
      throw new ConfigurationParseException("Access KEY is empty. Please provide valid access key");
    }
    if (apiKey == null && secretKey == null) {
      throw new ConfigurationParseException("Secret KEY is empty. Please provide valid secret key");
    }

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
        LOG.debug("Using proxy server {}:{} as user {} with password {} on "
            + "domain {} as workstation {}", clientConf.getProxyHost(),
              clientConf.getProxyPort(), String.valueOf(clientConf.getProxyUsername()),
              clientConf.getProxyPassword(), clientConf.getProxyDomain(),
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
    AWSStaticCredentialsProvider credProvider = null;
    if (apiKey != null) {
      String serviceInstanceID = props.getProperty(IAM_SERVICE_INSTANCE_ID_PROPERTY);
      String iamEndpoint = props.getProperty(IAM_ENDPOINT_PROPERTY);
      if (iamEndpoint != null) {
        SDKGlobalConfiguration.IAM_ENDPOINT = iamEndpoint;
      }

      BasicIBMOAuthCredentials creds = new BasicIBMOAuthCredentials(apiKey, serviceInstanceID);
      credProvider = new AWSStaticCredentialsProvider(creds);
    } else {
      BasicAWSCredentials creds =
          new BasicAWSCredentials(accessKey, secretKey);
      credProvider = new AWSStaticCredentialsProvider(creds);
    }
    final String serviceUrl = props.getProperty(ENDPOINT_URL_COS_PROPERTY);

    AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
        .withClientConfiguration(clientConf)
        .withPathStyleAccessEnabled(true)
        .withCredentials(credProvider);

    if (serviceUrl != null && !serviceUrl.equals(amazonDefaultEndpoint)) {
      EndpointConfiguration endpointConfiguration = new EndpointConfiguration(serviceUrl,
          Regions.DEFAULT_REGION.getName());
      clientBuilder.withEndpointConfiguration(endpointConfiguration);

    } else {
      clientBuilder.withRegion(Regions.DEFAULT_REGION);
    }
    mClient = clientBuilder.build();

    // Set block size property
    String mBlockSizeString = props.getProperty(BLOCK_SIZE_COS_PROPERTY, "128");
    mBlockSize = Long.valueOf(mBlockSizeString).longValue() * 1024 * 1024L;

    boolean autoCreateBucket =
        "true".equalsIgnoreCase((props.getProperty(AUTO_BUCKET_CREATE_COS_PROPERTY, "false")));

    partSize = Utils.getLong(conf, FS_COS, FS_ALT_KEYS, MULTIPART_SIZE,
        DEFAULT_MULTIPART_SIZE);
    multiPartThreshold = Utils.getLong(conf, FS_COS, FS_ALT_KEYS,
        MIN_MULTIPART_THRESHOLD, DEFAULT_MIN_MULTIPART_THRESHOLD);

    initTransferManager();

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
      } catch (Exception e) {
        LOG.error(e.getMessage());
        throw (e);
      }
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
   * Request object metadata; increments counters in the process.
   *
   * @param key key
   * @return the metadata
   */
  protected ObjectMetadata getObjectMetadata(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucket, key);
      return meta;
    } catch (AmazonClientException e) {
      LOG.warn(e.getMessage());
      return null;
    }
  }

  @Override
  public FileStatus getObjectMetadata(String hostName,
      Path path, String msg) throws IOException, FileNotFoundException {
    LOG.trace("Get object metadata: {}, hostname: {}", path, hostName);
    /*
     * The requested path is equal to hostName. HostName is equal to
     * hostNameScheme, thus the container. Therefore we have no object to look
     * for and we return the FileStatus as a directory. Containers have to
     * lastModified.
     */
    if (path.toString().equals(hostName) || (path.toString().length() + 1 == hostName.length())) {
      return new FileStatus(0L, true, 1, mBlockSize, 0L, path);
    }
    String key = pathToKey(hostName, path);
    LOG.debug("Modified key is {}", key);
    if (key.contains(HADOOP_TEMPORARY)) {
      LOG.debug("getObjectMetadata on temp object {}. Return not found", key);
      throw new FileNotFoundException("Not found " + path.toString());
    }
    try {
      FileStatus fileStatus = getFileStatusKeyBased(key, path);
      if (fileStatus != null) {
        return fileStatus;
      }
      if (!key.endsWith("/")) {
        String newKey = key + "/";
        fileStatus = getFileStatusKeyBased(newKey, path);
        if (fileStatus != null) {
          return fileStatus;
        } else {
          key = maybeAddTrailingSlash(key);
          ListObjectsRequest request = new ListObjectsRequest();
          request.setBucketName(mBucket);
          request.setPrefix(key);
          request.setDelimiter("/");
          request.setMaxKeys(1);

          ObjectListing objects = mClient.listObjects(request);
          if (!objects.getCommonPrefixes().isEmpty() || !objects.getObjectSummaries().isEmpty()) {
            return new FileStatus(0, true, 1, 0, 0, path);
          } else if (key.isEmpty()) {
            LOG.debug("Found root directory");
            return new FileStatus(0, true, 1, 0, 0, path);
          }
        }
      }
    } catch (Exception e) {
      LOG.debug("Not found {}", path.toString());
      LOG.warn(e.getMessage());
      throw new FileNotFoundException("Not found " + path.toString());
    }
    throw new FileNotFoundException("Not found " + path.toString());
  }

  private FileStatus getFileStatusKeyBased(String key, Path path) throws IOException {
    LOG.trace("Get file status by key {}, path {}", key, path);
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucket, key);
      return createFileStatus(meta.getContentLength(), key, meta.getLastModified(), path);
    } catch (AmazonServiceException e) {
      if (e.getStatusCode() != 404) {
        throw new IOException("getFileStatus " + key, e);
      }
    } catch (AmazonClientException e) {
      throw new IOException("getFileStatus " + key, e);
    }
    return null;
  }

  private FileStatus getFileStatusObjSummaryBased(S3ObjectSummary objSummary,
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
    String objName = path.toString();
    if (path.toString().startsWith(hostName)) {
      objName = path.toString().substring(hostName.length());
    }
    if (objName.contains(HADOOP_TEMPORARY)) {
      LOG.debug("Exists on temp object {}. Return false", objName);
      return false;
    }
    try {
      if (getObjectMetadata(hostName, path, "exists") != null) {
        return true;
      }
    } catch (FileNotFoundException e) {
      return false;
    }
    return false;
  }

  @Override
  public FSDataInputStream getObject(String hostName, Path path) throws IOException {
    String key = pathToKey(hostName, path);
    COSInputStream inputStream = new COSInputStream(mBucket, key, mBlockSize, mClient);
    return new FSDataInputStream(inputStream);
  }

  @Override
  public FSDataOutputStream createObject(String objName, String contentType,
      Map<String, String> metadata,
      Statistics statistics) throws IOException {
    try {
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

  public void setStocatorPath(StocatorPath sp) {
    stocatorPath = sp;
  }

  @Override
  public String getScheme() {
    return schemaProvided;
  }

  @Override
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException {
    String obj = path.toString();
    if (path.toString().startsWith(hostName)) {
      obj = path.toString().substring(hostName.length());
    }
    LOG.debug("Object name to delete {}. Path {}", obj, path.toString());
    try {
      getObjectMetadata(hostName, path, "delete");
    } catch (FileNotFoundException ex) {
      LOG.warn("Delete on {} not found. Nothing to delete");
      return false;
    }
    mClient.deleteObject(new DeleteObjectRequest(mBucket, obj));
    return true;
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
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  public Path getWorkingDirectory() {
    return workingDir;
  }

  public FileStatus[] list(String hostName, Path path, boolean fullListing,
      boolean prefixBased) throws IOException {
    String key = pathToKey(hostName, path);
    ArrayList<FileStatus> tmpResult = new ArrayList<FileStatus>();
    ListObjectsRequest request = new ListObjectsRequest().withBucketName(mBucket).withPrefix(key);

    String curObj;
    if (path.toString().equals(mBucket)) {
      curObj = "";
    } else if (path.toString().startsWith(mBucket + "/")) {
      curObj = path.toString().substring(mBucket.length() + 1);
    } else if (path.toString().startsWith(hostName)) {
      curObj = path.toString().substring(hostName.length());
    } else {
      curObj = path.toString();
    }

    ObjectListing objectList = mClient.listObjects(request);
    List<S3ObjectSummary> objectSummaries = objectList.getObjectSummaries();
    if (objectSummaries.size() == 0) {
      FileStatus[] emptyRes = {};
      LOG.debug("List for bucket {} is empty", mBucket);
      return emptyRes;
    }
    boolean objectScanContinue = true;
    S3ObjectSummary prevObj = null;
    while (objectScanContinue) {
      for (S3ObjectSummary obj : objectSummaries) {
        if (prevObj == null) {
          prevObj = obj;
          continue;
        }
        String objKey = obj.getKey();
        String unifiedObjectName = extractUnifiedObjectName(objKey);
        if (!prefixBased && !curObj.equals("") && !path.toString().endsWith("/")
            && !unifiedObjectName.equals(curObj) && !unifiedObjectName.startsWith(curObj + "/")) {
          LOG.trace("{} does not match {}. Skipped", unifiedObjectName, curObj);
          continue;
        }
        if (isSparkOrigin(unifiedObjectName) && !fullListing) {
          LOG.trace("{} created by Spark", unifiedObjectName);
          if (!isJobSuccessful(unifiedObjectName)) {
            LOG.trace("{} created by failed Spark job. Skipped", unifiedObjectName);
            if (fModeAutomaticDelete) {
              delete(hostName, new Path(objKey), true);
            }
            continue;
          } else {
            // if we here - data created by spark and job completed
            // successfully
            // however there be might parts of failed tasks that
            // were not aborted
            // we need to make sure there are no failed attempts
            if (nameWithoutTaskID(objKey).equals(nameWithoutTaskID(prevObj.getKey()))) {
              // found failed that was not aborted.
              LOG.trace("Colisiion found between {} and {}", prevObj.getKey(), objKey);
              if (prevObj.getSize() < obj.getSize()) {
                LOG.trace("New candidate is {}. Removed {}", obj.getKey(), prevObj.getKey());
                prevObj = obj;
              }
              continue;
            }
          }
        }
        if (prevObj.getSize() > 0 || fullListing) {
          FileStatus fs = getFileStatusObjSummaryBased(prevObj, hostName, path);
          tmpResult.add(fs);
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
    if (prevObj != null && (prevObj.getSize() > 0 || fullListing)) {
      FileStatus fs = getFileStatusObjSummaryBased(prevObj, hostName, path);
      tmpResult.add(fs);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("COS List to return length {}", tmpResult.size());
      for (FileStatus fs: tmpResult) {
        LOG.trace("{}", fs.getPath());
      }
    }
    return tmpResult.toArray(new FileStatus[tmpResult.size()]);
  }

  /**
   * Merge between two paths
   *
   * @param hostName
   * @param p path
   * @param objectName
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
   * @host hostName host of the object
   * @param path object full path
   */
  private String pathToKey(String hostName, Path path) {
    if (!path.isAbsolute()) {
      String pathStr = path.toUri().getPath();
      if (pathStr.startsWith(mBucket) && !pathStr.equals(mBucket)) {
        path = new Path(pathStr.substring(mBucket.length() + 1));
      }
      path = new Path(hostName, path);
    }

    if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
      return "";
    }

    return path.toUri().getPath().substring(1);
  }

  /**
   * Checks if container/object contains container/object/_SUCCESS If so, this
   * object was created by successful Hadoop job
   *
   * @param objectKey
   * @return boolean if job is successful
   */
  private boolean isJobSuccessful(String objectKey) {
    if (mCachedSparkJobsStatus.containsKey(objectKey)) {
      return mCachedSparkJobsStatus.get(objectKey).booleanValue();
    }
    String key = getRealKey(objectKey);
    String statusKey = String.format("%s/%s", key, HADOOP_SUCCESS);
    ObjectMetadata statusMetadata = getObjectMetadata(statusKey);
    Boolean isJobOK = Boolean.FALSE;
    if (statusMetadata != null) {
      isJobOK = Boolean.TRUE;
    }
    mCachedSparkJobsStatus.put(objectKey, isJobOK);
    return isJobOK.booleanValue();
  }

  /**
   * Accepts any object name. If object name of the form
   * a/b/c/gil.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_20160317132wrong_0000_m_000000_1 Then a/b/c/gil.data is
   * returned. Code testing that attempt_20160317132wrong_0000_m_000000_1 is
   * valid task id identifier
   *
   * @param objectKey
   * @return unified object name
   */
  private String extractUnifiedObjectName(String objectKey) {
    return extractFromObjectKeyWithTaskID(objectKey, true);
  }

  /**
   * Accepts any object name. If object name is of the form
   * a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_20160317132wrong_0000_m_000000_1 Then
   * a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv is
   * returned. Perform test that attempt_20160317132wrong_0000_m_000000_1 is
   * valid task id identifier
   *
   * @param objectName
   * @return unified object name
   */
  private String nameWithoutTaskID(String objectKey) {
    return extractFromObjectKeyWithTaskID(objectKey, false);
  }

  /**
   * Extracts from the object key an unified object name or name without task ID
   *
   * @param objectKey
   * @param isUnifiedObjectKey
   * @return
   */
  private String extractFromObjectKeyWithTaskID(String objectKey, boolean isUnifiedObjectKey) {
    Path p = new Path(objectKey);
    int index = objectKey.indexOf("-" + HADOOP_ATTEMPT);
    if (index > 0) {
      String attempt = objectKey.substring(objectKey.lastIndexOf("-") + 1);
      try {
        TaskAttemptID.forName(attempt);
        if (isUnifiedObjectKey) {
          return p.getParent().toString();
        } else {
          return objectKey.substring(0, index);
        }
      } catch (IllegalArgumentException e) {
        return objectKey;
      }
    } else if (isUnifiedObjectKey && objectKey.indexOf(HADOOP_SUCCESS) > 0) {
      return p.getParent().toString();
    }
    return objectKey;
  }

  /**
   * Checks if container/object exists and verifies that it contains
   * Data-Origin=stocator metadata If so, object was created by Spark.
   *
   * @param objectName
   * @return boolean if object was created by Spark
   */
  private boolean isSparkOrigin(String objectKey) {
    LOG.trace("check spark origin for {}", objectKey);
    if (mCachedSparkOriginated.containsKey(objectKey)) {
      return mCachedSparkOriginated.get(objectKey).booleanValue();
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
    mCachedSparkOriginated.put(objectKey, sparkOriginated);
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

    String userAgentName = singletoneInitTimeData.getUserAgentName();
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

    transfers = new TransferManager(mClient, threadPoolExecutor);
    transfers.setConfiguration(transferConfiguration);
  }

  synchronized File createTmpFileForWrite(String pathStr, long size) throws IOException {
    LOG.trace("Create temp file for write {}. size {}", pathStr, size);
    if (directoryAllocator == null) {
      String bufferDir = !Utils.getTrimmed(conf, FS_COS,
          FS_ALT_KEYS, BUFFER_DIR, "").isEmpty()
          ? BUFFER_DIR : "hadoop.tmp.dir";
      LOG.trace("Local buffer directorykey is {}", bufferDir);
      directoryAllocator = new LocalDirAllocator(bufferDir);
    }
    return directoryAllocator.createTmpFileForWrite(pathStr, size, conf);
  }

}
