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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.PaginationMap;
import org.javaswift.joss.model.StoredObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.swift.auth.DummyAccessProvider;
import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.auth.PasswordScopeAccessProvider;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PASSWORD_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.KEYSTONE_V3_AUTH;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_AUTH_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_REGION_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_USERNAME_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_TENANT_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_AUTH_METHOD_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_CONTAINER_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PUBLIC_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_BLOCK_SIZE_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_PROJECT_ID_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.SWIFT_USER_ID_PROPERTY;
import static com.ibm.stocator.fs.swift.SwiftConstants.FMODE_AUTOMATIC_DELETE_PROPERTY;
import static com.ibm.stocator.fs.common.Constants.HADOOP_SUCCESS;
import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.swift.SwiftConstants.PUBLIC_ACCESS;

/**
 * Swift back-end driver
 *
 */
public class SwiftAPIClient implements IStoreClient {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftAPIClient.class);
  /*
   * Time pattern
   */
  private static final String TIME_PATTERN = "EEE, d MMM yyyy hh:mm:ss zzz";
  /*
   * root container
   */
  private String container;
  /*
   * should use public or private URL
   */
  private boolean usePublicURL;
  /*
   * JOSS account object
   */
  private JossAccount mJossAccount;
  /*
   * block size
   */
  private long blockSize;

  /*
   * If true, automatic delete will be activated on the
   * data generated from failed tasks
   */
  private boolean fModeAutomaticDelete;

  /*
   * Contains map of object names that were written by Spark.
   * Used in container listing
   */
  private Map<String, Boolean> cachedSparkOriginated;

  /*
   * Contains map of objects that were created by successfull Spark jobs.
   * Used in container listing
   */
  private Map<String, Boolean> cachedSparkJobsStatus;

  /*
   * Page size for container listing
   */
  private final int pageListSize = 100;

  /*
   * support for different schema models
   */
  private String schemaProvided;

  /*
   * Keystone preferred region
   */
  private String preferredRegion;

  /*
   * file system URI
   */
  private final URI filesystemURI;

  /*
   * Hadoop configuration
   */
  private final Configuration conf;

  /**
   * Constructor method
   *
   * @param pFilesystemURI The URI to the object store
   * @param pConf Configuration
   * @throws IOException when initialization is failed
   */
  public SwiftAPIClient(URI pFilesystemURI, Configuration pConf) throws IOException {
    conf = pConf;
    filesystemURI = pFilesystemURI;
  }

  @Override
  public void initiate() throws IOException {
    cachedSparkOriginated = new HashMap<String, Boolean>();
    cachedSparkJobsStatus = new HashMap<String, Boolean>();
    schemaProvided = conf.get("fs.swift.schema", Constants.SWIFT2D);

    Properties props = ConfigurationHandler.initialize(filesystemURI, conf);
    AccountConfig config = new AccountConfig();
    fModeAutomaticDelete = "true".equals(props.getProperty(FMODE_AUTOMATIC_DELETE_PROPERTY,
        "false"));
    blockSize = Long.valueOf(props.getProperty(SWIFT_BLOCK_SIZE_PROPERTY,
        "128")).longValue() * 1024 * 1024L;
    String authMethod = props.getProperty(SWIFT_AUTH_METHOD_PROPERTY);
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);

    if (authMethod.equals(PUBLIC_ACCESS)) {
      setupPublicAccount(filesystemURI, config);
    } else {
      setupPrivateAccount(props, config, authMethod);
    }

    mJossAccount.authenticate();
    Container containerObj = mJossAccount.getAccount().getContainer(container);

    if (!containerObj.exists() && !authMethod.equals(PUBLIC_ACCESS)) {
      containerObj.create();
    }
  }

  private void setupPrivateAccount(Properties props, AccountConfig config,
                                             String authMethod) throws IOException {
    container = props.getProperty(SWIFT_CONTAINER_PROPERTY);
    String isPubProp = props.getProperty(SWIFT_PUBLIC_PROPERTY, "false");
    usePublicURL = "true".equals(isPubProp);
    config.setPassword(props.getProperty(SWIFT_PASSWORD_PROPERTY));
    config.setAuthUrl(Utils.getOption(props, SWIFT_AUTH_PROPERTY));

    if (authMethod.equals("keystone")) {
      preferredRegion = props.getProperty(SWIFT_REGION_PROPERTY);
      if (preferredRegion != null) {
        config.setPreferredRegion(preferredRegion);
      }
      config.setAuthenticationMethod(AuthenticationMethod.KEYSTONE);
      config.setUsername(Utils.getOption(props, SWIFT_USERNAME_PROPERTY));
      config.setTenantName(props.getProperty(SWIFT_TENANT_PROPERTY));
    } else if (authMethod.equals(KEYSTONE_V3_AUTH)) {
      preferredRegion = props.getProperty(SWIFT_REGION_PROPERTY, "dallas");
      config.setPreferredRegion(preferredRegion);
      config.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
      String userId = props.getProperty(SWIFT_USER_ID_PROPERTY);
      String projectId = props.getProperty(SWIFT_PROJECT_ID_PROPERTY);
      PasswordScopeAccessProvider psap = new PasswordScopeAccessProvider(userId,
              config.getPassword(), projectId, config.getAuthUrl(), preferredRegion);
      config.setAccessProvider(psap);
    } else {
      config.setAuthenticationMethod(AuthenticationMethod.TEMPAUTH);
      config.setTenantName(Utils.getOption(props, SWIFT_USERNAME_PROPERTY));
      config.setUsername(props.getProperty(SWIFT_TENANT_PROPERTY));
    }
    mJossAccount = new JossAccount(config,preferredRegion, usePublicURL);
    mJossAccount.createAccount();
  }

  private void setupPublicAccount(URI filesystemURI, AccountConfig config)
          throws IOException {

    // we need to extract container name and path from the public URL
    String publicURL = filesystemURI.toString().replace("swift2d", "https");
    LOG.debug("publicURL: {}", publicURL);
    String accessURL = Utils.extractAccessURL(publicURL);
    LOG.debug("auth url {}", accessURL);
    config.setAuthUrl(accessURL);
    config.setAuthenticationMethod(AuthenticationMethod.EXTERNAL);
    container = Utils.extractDataRoot(publicURL, accessURL);
    DummyAccessProvider p = new DummyAccessProvider(accessURL);
    config.setAccessProvider(p);
    mJossAccount = new JossAccount(config, null, true);
    mJossAccount.createDummyAccount();
  }

  @Override
  public String getScheme() {
    return schemaProvided;
  }

  @Override
  public String getDataRoot() {
    return container;
  }

  public long getBlockSize() {
    return blockSize;
  }

  @Override
  public FileStatus getObjectMetadata(String hostName,
      Path path) throws IOException, FileNotFoundException {
    LOG.trace("Get object metadata: {}, hostname: {}", path, hostName);
    Container cont = mJossAccount.getAccount().getContainer(container);
    /*
      The requested path is equal to hostName.
      HostName is equal to hostNameScheme, thus the container.
      Therefore we have no object to look for and
      we return the FileStatus as a directory.
      Containers have to lastModified.
     */
    if (path.toString().equals(hostName) || (path.toString().length() + 1 == hostName.length())) {
      LOG.debug("Object metadata requested on container!");
      return new FileStatus(0L, true, 1, blockSize, 0L, path);
    }
    /*
      The requested path is not equal to the container.
      We need to check if the object requested is a real object or a directory.
      This may be triggered when users want to access a directory rather than
      the entire container.
      A directory in Swift can have two implementations:
      1) a zero byte object with the name of the directory
      2) no zero byte object with the name of the directory
    */
    String objectName = path.toString().substring(hostName.length());
    if (objectName.endsWith("/")) {
      /*
        removing the trailing slash because it is not supported in Swift
        an request on an object (not a container) that has a trailing slash will lead
        to a 404 response message
      */
      objectName = objectName.substring(0, objectName.length() - 1);
    }
    StoredObject so = cont.getObject(objectName);
    boolean isDirectory = false;
    if (so.exists()) {
      // We need to check if the object size is equal to zero
      // If so, it might be a directory
      long contentLength = so.getContentLength();
      String lastModified = so.getLastModified();
      if (contentLength == 0) {
        Collection<DirectoryOrObject> directoryFiles = cont.listDirectory(objectName, '/', "", 10);
        if (directoryFiles != null && directoryFiles.size() != 0) {
          // The zero length object is a directory
          isDirectory = true;
        }
      }
      LOG.trace("Got object. isDirectory: {}  lastModified: {}", isDirectory, lastModified);
      return new FileStatus(contentLength, isDirectory, 1, blockSize,
              getLastModified(lastModified), path);
    }
    // We need to check if it may be a directory with no zero byte file associated
    Collection<DirectoryOrObject> directoryFiles = cont.listDirectory(objectName, '/', "", 10);
    if (directoryFiles != null && directoryFiles.size() != 0) {
      // In this case there is no lastModified
      LOG.debug("Got object. isDirectory: {}  lastModified: {}", isDirectory, null);
      return new FileStatus(0, isDirectory, 1, blockSize, 0L, path);
    }
    LOG.debug("Not found {}", path.toString());
    return null;
  }

  /**
   * Transforms last modified time stamp from String to the long format
   *
   * @param strTime time in string format as returned from Swift
   * @return time in long format
   * @throws IOException if failed to parse time stamp
   */
  private long getLastModified(String strTime) throws IOException {
    final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TIME_PATTERN);
    try {
      long lastModified = simpleDateFormat.parse(strTime).getTime();
      if (lastModified == 0) {
        lastModified = System.currentTimeMillis();
      }
      return lastModified;
    } catch (ParseException e) {
      throw new IOException("Failed to parse " + strTime, e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * Will return false for any object with _temporary.
   * No need to HEAD Swift, since _temporary objects are not exists in Swift.
   *
   * @param hostName host name
   * @param path path to the data
   * @return true if object exists
   * @throws IOException if error
   * @throws FileNotFoundException if error
   */
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
    StoredObject so = mJossAccount.getAccount().getContainer(container)
        .getObject(objName);
    return so.exists();
  }

  public FSDataInputStream getObject(String hostName, Path path) throws IOException {
    LOG.debug("Get object: {}", path);
    String objName = path.toString();
    if (path.toString().startsWith(hostName)) {
      objName = path.toString().substring(hostName.length());
    }
    URL url = new URL(mJossAccount.getAccessURL() + "/" + container + "/" + objName);

    FileStatus fs = getObjectMetadata(hostName, path);
    SwiftInputStream sis = new SwiftInputStream(url.toString(),
        fs.getLen(), mJossAccount, Constants.NORMAL_READ_STRATEGY);
    return new FSDataInputStream(sis);
  }

  /**
   * {@inheritDoc}
   *
   * some examples of failed attempts:
   * a/b/c.data/part-00099-attempt_201603171503_0001_m_000099_119
   * a/b/c.data/part-00099-attempt_201603171503_0001_m_000099_120
   * a/b/c.data/part-00099-attempt_201603171503_0001_m_000099_121
   * a/b/c.data/part-00099-attempt_201603171503_0001_m_000099_122
   * or
   * a/b/c.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_201603171328_0000_m_000000_1
   * a/b/c.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   * .csv-attempt_201603171328_0000_m_000000_0
   * in all the cases format is objectname-taskid where
   * taskid may vary, depends how many tasks were re-submitted

   * @param hostName hostname
   * @param path path to the object
   * @param fullListing if true, will return objects of size 0
   * @return Array of Hadoop FileStatus
   * @throws IOException in case of network failure
   */
  public FileStatus[] list(String hostName, Path path, boolean fullListing) throws IOException {
    LOG.debug("List container: raw path parent", path.toString());
    Container cObj = mJossAccount.getAccount().getContainer(container);
    String obj;
    if (path.toString().startsWith(container)) {
      obj = path.toString().substring(container.length() + 1);
    } else {
      if (path.toString().length() + 1 == hostName.length()) {
        obj = "";
      } else {
        obj = path.toString().substring(hostName.length());
      }
    }
    LOG.debug("List container for {} container {}", obj, container);
    ArrayList<FileStatus> tmpResult = new ArrayList<FileStatus>();
    PaginationMap paginationMap = cObj.getPaginationMap(obj, pageListSize);
    FileStatus fs = null;
    StoredObject previousElement = null;
    for (Integer page = 0; page < paginationMap.getNumberOfPages(); page++) {
      Collection<StoredObject> res = cObj.list(paginationMap, page);
      if (page == 0 && (res == null || res.isEmpty())) {
        FileStatus[] emptyRes = {};
        LOG.debug("List {} in container {} is empty", obj, container);
        return emptyRes;
      }
      for (StoredObject tmp : res) {
        if (previousElement == null) {
          // first entry
          setCorrectSize(tmp, cObj);
          previousElement = tmp.getAsObject();
          continue;
        }
        String unifiedObjectName = extractUnifiedObjectName(tmp.getName());
        if (isSparkOrigin(unifiedObjectName) && !fullListing) {
          LOG.trace("{} created by Spark", unifiedObjectName);
          if (!isJobSuccessful(unifiedObjectName)) {
            LOG.trace("{} created by failed Spark job. Skipped", unifiedObjectName);
            if (fModeAutomaticDelete) {
              delete(hostName, new Path(tmp.getName()), true);
            }
            continue;
          } else {
            // if we here - data created by spark and job completed successfully
            // however there be might parts of failed tasks that were not aborted
            // we need to make sure there are no failed attempts
            if (nameWithoutTaskID(tmp.getName())
                .equals(nameWithoutTaskID(previousElement.getName()))) {
              // found failed that was not aborted.
              LOG.trace("Colisiion found between {} and {}", previousElement.getName(),
                  tmp.getName());
              setCorrectSize(tmp, cObj);
              if (previousElement.getContentLength() < tmp.getContentLength()) {
                LOG.trace("New candidate is {}. Removed {}", tmp.getName(),
                    previousElement.getName());
                previousElement = tmp.getAsObject();
              }
              continue;
            }
          }
        }
        fs = null;
        if (previousElement.getContentLength() > 0 || fullListing) {
          fs = getFileStatus(previousElement, cObj, hostName, path);
          tmpResult.add(fs);
        }
        previousElement = tmp.getAsObject();
      }
    }
    if (previousElement != null && (previousElement.getContentLength() > 0 || fullListing)) {
      fs = getFileStatus(previousElement, cObj, hostName, path);
      tmpResult.add(fs);
    }
    LOG.debug("Listing of {} completed with {} results", path.toString(), tmpResult.size());
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
  private String getMergedPath(String hostName, Path p, String objectName) {
    if ((p.getParent() != null) && (p.getName() != null)
        && (p.getParent().toString().equals(hostName))) {
      if (objectName.equals(p.getName())) {
        return p.toString();
      }
      if (objectName.startsWith(p.getName())) {
        return p.getParent() + objectName;
      }
      return p.toString();
    }
    return hostName + objectName;
  }

  /**
   * Direct HTTP PUT request without JOSS package
   *
   * @param objName name of the object
   * @param contentType content type
   * @return HttpURLConnection
   */
  @Override
  public FSDataOutputStream createObject(String objName, String contentType,
      Map<String, String> metadata, Statistics statistics) throws IOException {
    URL url = new URL(mJossAccount.getAccessURL() + "/" + objName);
    LOG.debug("PUT {}. Content-Type : {}", url.toString(), contentType);
    try {
      return new FSDataOutputStream(new SwiftOutputStream(mJossAccount, url, contentType, metadata),
          statistics);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException {
    String obj = path.toString();
    if (path.toString().startsWith(hostName)) {
      obj = path.toString().substring(hostName.length());
    }
    LOG.debug("Object name to delete {}. Path {}", obj, path.toString());
    StoredObject so = mJossAccount.getAccount().getContainer(container)
        .getObject(obj);
    if (so.exists()) {
      so.delete();
    }
    return true;
  }

  @Override
  public URI getAccessURI() throws IOException {
    return URI.create(mJossAccount.getAccessURL());
  }

  /**
   * {@inheritDoc}
   *
   * Swift driver doesn't require any local working directory
   *
   * @return path to the working directory
   */
  @Override
  public Path getWorkingDirectory() {
    return null;
  }

  /**
   * Checks if container/object exists and verifies
   * that it contains Data-Origin=stocator metadata
   * If so, object was created by Spark.
   *
   * @param objectName
   * @return boolean if object was created by Spark
   */
  private boolean isSparkOrigin(String objectName) {
    if (cachedSparkOriginated.containsKey(objectName)) {
      return cachedSparkOriginated.get(objectName).booleanValue();
    }
    String obj = objectName;
    if (objectName.toString().startsWith(container)) {
      obj = objectName.substring(container.length() + 1);
    }
    Boolean sparkOriginated = Boolean.FALSE;
    StoredObject so = mJossAccount.getAccount().getContainer(container).getObject(obj);
    if (so.exists()) {
      Object sparkOrigin = so.getMetadata("Data-Origin");
      if (sparkOrigin != null) {
        String tmp = (String) sparkOrigin;
        if (tmp.equals("stocator")) {
          sparkOriginated = Boolean.TRUE;
        }
      }
    }
    cachedSparkOriginated.put(objectName, sparkOriginated);
    return sparkOriginated.booleanValue();
  }

  /**
   * Checks if container/object contains
   * container/object/_SUCCESS
   * If so, this object was created by successful Hadoop job
   *
   * @param objectName
   * @return boolean if job is successful
   */
  private boolean isJobSuccessful(String objectName) {
    if (cachedSparkJobsStatus.containsKey(objectName)) {
      return cachedSparkJobsStatus.get(objectName).booleanValue();
    }
    String obj = objectName;
    if (objectName.toString().startsWith(container)) {
      obj = objectName.substring(container.length() + 1);
    }
    Account account = mJossAccount.getAccount();
    StoredObject so = account.getContainer(container).getObject(obj
        + "/" + HADOOP_SUCCESS);
    Boolean isJobOK = Boolean.FALSE;
    if (so.exists()) {
      isJobOK = Boolean.TRUE;
    }
    cachedSparkJobsStatus.put(objectName, isJobOK);
    return isJobOK.booleanValue();
  }

  /**
   * Accepts any object name.
   * If object name of the form
   * a/b/c/gil.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   *    .csv-attempt_20160317132a_wrong_0000_m_000000_1
   * Then a/b/c/gil.data is returned.
   * Code testing that attempt_20160317132a_wrong_0000_m_000000_1 is valid
   * task id identifier
   *
   * @param objectName
   * @return unified object name
   */
  private String extractUnifiedObjectName(String objectName) {
    Path p = new Path(objectName);
    if (objectName.indexOf("-" + HADOOP_ATTEMPT) > 0) {
      String attempt = objectName.substring(objectName.lastIndexOf("-") + 1);
      try {
        TaskAttemptID.forName(attempt);
        return p.getParent().toString();
      } catch (IllegalArgumentException e) {
        return objectName;
      }
    } else if (objectName.indexOf(HADOOP_SUCCESS) > 0) {
      return p.getParent().toString();
    }
    return objectName;
  }

  /**
   * Accepts any object name.
   * If object name is of the form
   * a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c
   *    .csv-attempt_20160317132a_wrong_0000_m_000000_1
   * Then a/b/c/m.data/part-r-00000-48ae3461-203f-4dd3-b141-a45426e2d26c.csv is returned.
   * Perform test that attempt_20160317132a_wrong_0000_m_000000_1 is valid
   * task id identifier
   *
   * @param objectName
   * @return unified object name
   */
  private String nameWithoutTaskID(String objectName) {
    int index = objectName.indexOf("-" + HADOOP_ATTEMPT);
    if (index > 0) {
      String attempt = objectName.substring(objectName.lastIndexOf("-") + 1);
      try {
        TaskAttemptID.forName(attempt);
        return objectName.substring(0, index);
      } catch (IllegalArgumentException e) {
        return objectName;
      }
    }
    return objectName;
  }

  /**
   * Swift has a bug where container listing might wrongly report size 0
   * for large objects. It's seems to be a well known issue in Swift without
   * solution.
   * We have to provide work around for this.
   * If container listing reports size 0 for some object, we send
   * additional HEAD on that object to verify it's size.
   *
   * @param tmp JOSS StoredObject
   * @param cObj JOSS Container object
   */
  private void setCorrectSize(StoredObject tmp, Container cObj) {
    long objectSize = tmp.getContentLength();
    if (objectSize == 0) {
      // we may hit a well known Swift bug.
      // container listing reports 0 for large objects.
      StoredObject soDirect = cObj
          .getObject(tmp.getName());
      if (soDirect.getContentLength() > 0) {
        tmp.setContentLength(soDirect.getContentLength());
      }
    }
  }

  /**
   * Maps StoredObject of JOSS into Hadoop FileStatus
   *
   * @param tmp Stored Object
   * @param cObj Container Object
   * @param hostName host name
   * @param path path to the object
   * @return FileStatus representing current object
   * @throws IllegalArgumentException if error
   * @throws IOException if error
   */
  private FileStatus getFileStatus(StoredObject tmp, Container cObj,
      String hostName, Path path) throws IllegalArgumentException, IOException {
    String newMergedPath = getMergedPath(hostName, path, tmp.getName());
    return new FileStatus(tmp.getContentLength(), false, 1, blockSize,
        getLastModified(tmp.getLastModified()), 0, null,
        null, null, new Path(newMergedPath));
  }

  @Override
  public boolean rename(String hostName, String srcPath, String dstPath) throws IOException {
    LOG.debug("Rename from {} to {}. hostname is {}", srcPath, dstPath, hostName);
    String objNameSrc = srcPath.toString();
    if (srcPath.toString().startsWith(hostName)) {
      objNameSrc = srcPath.toString().substring(hostName.length());
    }
    String objNameDst = dstPath.toString();
    if (objNameDst.toString().startsWith(hostName)) {
      objNameDst = dstPath.toString().substring(hostName.length());
    }

    if (objNameSrc.contains(HADOOP_TEMPORARY)) {
      LOG.debug("Exists on temp object {}. Return false", objNameSrc);
      return true;
    }
    LOG.debug("Rename modified from {} to {}", objNameSrc, objNameDst);
    Container cont = mJossAccount.getAccount().getContainer(container);
    StoredObject so = cont.getObject(objNameSrc);
    StoredObject soDst = cont.getObject(objNameDst);
    so.copyObject(cont, soDst);
    return true;
  }
}
