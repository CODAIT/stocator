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
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.DirectoryOrObject;
import org.javaswift.joss.model.StoredObject;

import org.javaswift.joss.model.Directory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.Utils;
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
  private final String container;
  /*
   * should use public or private URL
   */
  private final boolean usePublicURL;
  /*
   * JOSS account object
   */
  private Account mAccount;
  /*
   * JOSS authentication object
   */
  private Access mAccess;
  /*
   * block size
   */
  private long blockSize;

  public SwiftAPIClient(URI filesystemURI, Configuration conf) throws IOException {
    LOG.debug("Init : {}", filesystemURI.toString());
    String preferredRegion = null;
    Properties props = ConfigurationHandler.initialize(filesystemURI, conf);
    container = props.getProperty(SWIFT_CONTAINER_PROPERTY);
    String isPubProp = props.getProperty(SWIFT_PUBLIC_PROPERTY, "false");
    usePublicURL = "true".equals(isPubProp);
    blockSize = Long.valueOf(props.getProperty(SWIFT_BLOCK_SIZE_PROPERTY,
        "128")).longValue() * 1024 * 1024L;
    AccountConfig config = new AccountConfig();
    config.setPassword(props.getProperty(SWIFT_PASSWORD_PROPERTY));
    config.setAuthUrl(Utils.getOption(props, SWIFT_AUTH_PROPERTY));
    String authMethod = props.getProperty(SWIFT_AUTH_METHOD_PROPERTY);
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
    LOG.debug("JOSS configuration completed");
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.WRAP_ROOT_VALUE, true);
    mAccount = new AccountFactory(config).createAccount();
    mAccess = mAccount.authenticate();
    if (preferredRegion != null) {
      mAccess.setPreferredRegion(preferredRegion);
    }
    LOG.debug("Url: {}, Token: {}", mAccess.getPublicURL(), mAccess.getToken());
    Container containerObj = mAccount.getContainer(container);
    if (!containerObj.exists()) {
      containerObj.create();
    }
  }

  @Override
  public String getScheme() {
    return Constants.SWIFT;
  }

  @Override
  public String getDataRoot() {
    return container;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public Account getAccount() {
    return mAccount;
  }

  @Override
  public FileStatus getObjectMetadata(String hostName,
      Path path) throws IOException, FileNotFoundException {
    LOG.debug("Get object metadata: {}, hostname: {}", path, hostName);
    Container cont = mAccount.getContainer(container);
    /*
      The requested path is equal to hostName.
      HostName is equal to hostNameScheme, thus the container.
      Therefore we have no object to look for and
      we return the FileStatus as a directory.
      Containers have to lastModified.
     */
    if (path.toString().equals(hostName)) {
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
      LOG.debug("Got object. isDirectory: {}  lastModified: {}", isDirectory, lastModified);
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

    throw new FileNotFoundException(objectName + " does not exists");
  }

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

  public boolean exists(String hostName, Path path) throws IOException, FileNotFoundException {
    LOG.trace("Object exists: {}", path);
    StoredObject so = mAccount.getContainer(container)
        .getObject(path.toString().substring(hostName.length()));
    return so.exists();
  }

  public FSDataInputStream getObject(String hostName, Path path) throws IOException {
    LOG.debug("Get object metadata: {}", path);
    try {
      SwiftInputStream sis = new SwiftInputStream(this, hostName, path);
      return new FSDataInputStream(sis);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

  public FileStatus[] listContainer(String hostName, Path path) throws IOException {
    LOG.debug("List container: path parent: {}, name {}", path.getParent(), path.getName());
    Container cObj = mAccount.getContainer(container);
    ArrayList<FileStatus> tmpResult = new ArrayList<>();
    FileStatus fs;
    Collection<DirectoryOrObject> elements;

    if (path.getParent() == null) {
      elements = cObj.listDirectory();
      LOG.debug("Listing root directory");
    } else {
      Directory dir = new Directory(path.toString().substring(hostName.length())
                                    + Path.SEPARATOR, Path.SEPARATOR_CHAR);
      elements = cObj.listDirectory(dir);
      LOG.debug("Directory {} listed", dir.getName());
    }

    for (DirectoryOrObject e: elements) {
      String newMergedPath = getMergedPath(hostName, path, e.getName());
      if (e.isObject()) {
        fs = new FileStatus(e.getAsObject().getContentLength(), false, 1, blockSize,
                            getLastModified(e.getAsObject().getLastModified()), 0, null, null, null,
                            new Path(newMergedPath));
        LOG.debug("{} is an object", newMergedPath);
        if (fs.getLen() > 0) {
          tmpResult.add(fs);
          LOG.debug("{} added to results", e.getName());
        }
      } else {
        StoredObject success = cObj.getObject(e.getName() + Constants.HADOOP_SUCCESS);
        if (success.exists()) {
          fs = new FileStatus(0, true, 1, blockSize,
                              0, 0, null, null, null,
                              new Path(newMergedPath));
          LOG.debug("{} is a directory", newMergedPath);
          tmpResult.add(fs);
          LOG.debug("{} added to results", e.getName());
        }
      }
    }
    return tmpResult.toArray(new FileStatus[tmpResult.size()]);
  }

  private String getMergedPath(String hostName, Path p, String objectName) {
    if ((p.getParent() != null) && (p.getName() != null)
        && (p.getParent().toString().equals(hostName))) {
      if (objectName.equals(p.getName())) {
        return p.toString();
      }
      if (objectName.startsWith(p.getName())) {
        return p.getParent() + objectName;
      }
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
      Statistics statistics) throws IOException {
    URL url = new URL(getAccessURL() + "/" + objName);
    LOG.debug("PUT {}. Content-Type : {}", url.toString(), contentType);
    try {
      HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
      httpCon.setDoOutput(true);
      httpCon.setRequestMethod("PUT");
      httpCon.addRequestProperty("X-Auth-Token", getAuthToken());
      httpCon.addRequestProperty("Content-Type", contentType);
      return new FSDataOutputStream(new SwiftOutputStream(httpCon),
          statistics);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  @Override
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException {
    StoredObject so = mAccount.getContainer(container)
        .getObject(path.toString().substring(hostName.length()));
    if (so.exists()) {
      so.delete();
    }
    return true;
  }

  private String getAuthToken() {
    return mAccess.getToken();
  }

  /**
   * Get authenticated URL
   */
  private String getAccessURL() {
    if (usePublicURL) {
      return mAccess.getPublicURL();
    }
    return mAccess.getInternalURL();
  }

}
