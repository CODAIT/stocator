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

package com.ibm.stocator.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.ObjectStoreGlobber;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;

/**
 * Object store driver implementation
 * Based on the Hadoop FileSystem interface
 *
 */
public class ObjectStoreFileSystem extends FileSystem {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStoreFileSystem.class);

  /*
   * Storage client. Contains implementation of the underlying storage.
   */
  private IStoreClient storageClient;
  /*
   * Host name with schema, e.g. schema://dataroot.conf-entry/
   */
  private String hostNameScheme;

  @Override
  public String getScheme() {
    return storageClient.getScheme();
  }

  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);
    if (!conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
      throw new IOException("mapreduce.fileoutputcommitter.marksuccessfuljobs should be enabled");
    }
    setConf(conf);
    String nameSpace = fsuri.toString().substring(0, fsuri.toString().indexOf("://"));
    if (storageClient == null) {
      storageClient = ObjectStoreVisitor.getStoreClient(nameSpace, fsuri, conf);
      hostNameScheme = storageClient.getScheme() + "://"  + Utils.getHost(fsuri) + "/";
    }
  }

  @Override
  public URI getUri() {
    return null;
  }

  /**
   * Check path should check the validity of the path. Skipped at this point.
   */
  @Override
  protected void checkPath(Path path) {
    LOG.trace("Check path: {}", path.toString());
  }

  /**
   * Check if the object exists.
   */
  @Override
  public boolean exists(Path f) throws IOException {
    LOG.debug("exists {}", f.toString());
    return storageClient.exists(hostNameScheme, f);
  }

  /**
   * There is no "directories" in the object store
   * The general structure is "dataroot/object"
   * and "object" may contain nested structure
   */
  @Override
  public boolean isDirectory(Path f) throws IOException {
    LOG.debug("is directory: {}", f.toString());
    return false;
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
      throws FileNotFoundException, IOException {
    LOG.debug("listLocatedStatus: {} ", f.toString());
    return super.listLocatedStatus(f);
  }

  @Override
  protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f,
      PathFilter filter)
      throws FileNotFoundException, IOException {
    LOG.debug("listLocatedStatus with path filter: {}", f.toString());
    return super.listLocatedStatus(f, filter);
  }

  @Override
  public FSDataInputStream open(Path f) throws IOException {
    LOG.debug("open method: {} without buffer size" , f.toString());
    return storageClient.getObject(hostNameScheme, f);
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    LOG.debug("open method: {} with buffer size {}", f.toString(), bufferSize);
    return storageClient.getObject(hostNameScheme, f);
  }

  /**
   * {@inheritDoc}
   * create path of the form dataroot/objectname
   * Each object name is modified to contain task-id prefix.
   * Thus for example, create
   * dataroot/objectname/_temporary/0/_temporary/attempt_201603131849_0000_m_000019_0/
   * part-r-00019-a08dcbab-8a34-4d80-a51c-368a71db90aa.csv
   * will be transformed to
   * PUT dataroot/object
   * /201603131849_0000_m_000019_0-part-r-00019-a08dcbab-8a34-4d80-a51c-368a71db90aa.csv
   *
   * @param f
   * @param permission
   * @param overwrite
   * @param bufferSize
   * @param replication
   * @param blockSize
   * @param progress
   * @return FSDataOutputStream to write data in
   * @throws IOException
   */
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    LOG.debug("Create method: {}", f.toString());
    String objNameModified = "";
    // check if request is dataroot/objectname/_SUCCESS
    if (f.getName().equals(Constants.HADOOP_SUCCESS)) {
      objNameModified =  getObjectNameRoot(f, HADOOP_TEMPORARY, false);
    } else {
      objNameModified = getObjectNameRoot(f, HADOOP_TEMPORARY, true);
    }
    FSDataOutputStream outStream = storageClient.createObject(objNameModified,
        "binary/octet-stream", null, statistics);
    return outStream;
  }

  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Append is not supported");
  }

  /**
   * We don't need rename, since objects are already were created with real
   * names.
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    LOG.debug("rename from {} to {}", src.toString(), dst.toString());
    return true;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("delete method: {}. recursive {}", f.toString(), recursive);
    String objNameModified = getObjectNameRoot(f, HADOOP_TEMPORARY, true);
    LOG.debug("Modified object name {}", objNameModified);
    if (objNameModified.contains(HADOOP_TEMPORARY)) {
      return true;
    }
    Path pathToObj = new Path(objNameModified);
    if (f.getName().startsWith(HADOOP_ATTEMPT)) {
      FileStatus[] fsList = storageClient.list(hostNameScheme, pathToObj.getParent(), true);
      if (fsList.length > 0) {
        for (FileStatus fs: fsList) {
          if (fs.getPath().getName().endsWith(f.getName())) {
            storageClient.delete(hostNameScheme, fs.getPath(), recursive);
          }
        }
      }
    } else {
      FileStatus[] fsList = storageClient.list(hostNameScheme, pathToObj, true);
      if (fsList.length > 0) {
        for (FileStatus fs: fsList) {
          storageClient.delete(hostNameScheme, fs.getPath(), recursive);
        }
      }
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path f,
      PathFilter filter) throws FileNotFoundException, IOException {
    if (filter != null) {
      LOG.debug("list status: {}, filter: {}",f.toString(), filter.toString());
    } else {
      LOG.debug("list status: {}", f.toString());
    }
    FileStatus[] res = {};
    if (f.toString().contains(HADOOP_TEMPORARY)) {
      return res;
    }
    return storageClient.list(hostNameScheme, f, false);
  }

  @Override
  public FileStatus[] listStatus(Path[] files,
      PathFilter filter) throws FileNotFoundException, IOException {
    return super.listStatus(files, filter);
  }

  @Override
  public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
    return super.listStatus(files);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    LOG.debug("List status of {}", f.toString());
    FileStatus[] res = {};
    if (f.toString().contains(HADOOP_TEMPORARY)) {
      return res;
    }
    return storageClient.list(hostNameScheme, f, false);
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
      throws FileNotFoundException, IOException {
    LOG.debug("list files: {}", f.toString());
    return super.listFiles(f, recursive);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    LOG.debug("set working directory: {}", new_dir.toString());

  }

  /**
   * {@inheritDoc}
   *
   * When path is of the form schema://dataroot.provider/objectname/_temporary/0
   * it is assumed that new job started to write it's data.
   * In this case we create an empty object schema://dataroot.provider/objectname
   * that will later be used to identify objects that were created by Spark.
   * This is needed for fault tolerance coverage to identify data that was created
   * by failed jobs or tasks.
   * dataroot/object created as a 0 size object with type application/directory
   *
   * @param f path to create
   * @param permission
   * @return boolean on success or failure
   * @throws IOException
   */
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mkdirs(f);
  }

  /**
   * {@inheritDoc}
   *
   * When path is of the form schema://dataroot.provider/objectname/_temporary/0
   * it is assumed that new job started to write it's data.
   * In this case we create an empty object schema://dataroot.provider/objectname
   * that will later be used to identify objects that were created by Spark.
   * This is needed for fault tolerance coverage to identify data that was created
   * by failed jobs or tasks.
   * dataroot/object created as a 0 size object with type application/directory
   *
   * @param f path to create
   * @return boolean on success or failure
   * @throws IOException
   */
  @Override
  public boolean mkdirs(Path f) throws IOException {
    LOG.debug("mkdirs: {}", f.toString());
    if (f.getParent().toString().endsWith(HADOOP_TEMPORARY)) {
      String objNameModified = getObjectNameRoot(f, HADOOP_TEMPORARY, true);
      Path pathToObj = new Path(objNameModified);
      String plainObjName = pathToObj.getParent().toString();
      LOG.debug("Going to create identifier {}", plainObjName);
      Map<String, String> metadata = new HashMap<String, String>();
      metadata.put("Data-Origin", "stocator");
      FSDataOutputStream outStream = storageClient.createObject(plainObjName,
          "application/directory", metadata, statistics);
      outStream.close();
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    LOG.debug("get file status: {}", f.toString());
    return storageClient.getObjectMetadata(hostNameScheme, f);
  }

  @Override
  public Path resolvePath(Path p) throws IOException {
    LOG.debug("resolve path: {}", p.toString());
    return super.resolvePath(p);
  }

  @Override
  public long getBlockSize(Path f) throws IOException {
    LOG.debug("get block size: {}", f.toString());
    return getFileStatus(f).getBlockSize();
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    LOG.debug("get content summary: {}", f.toString());
    return super.getContentSummary(f);
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    LOG.trace("Get default block size for: {}", f.toString());
    return super.getDefaultBlockSize(f);
  }

  /**
   * Extract object name from path. If addTaskIdCompositeName=true then
   * schema://tone1.lvm/aa/bb/cc/one3.txt/_temporary/0/_temporary/
   * attempt_201610052038_0001_m_000007_15/part-00007 will extract get
   * aa/bb/cc/201610052038_0001_m_000007_15-one3.txt
   * otherwise object name will be aa/bb/cc/one3.txt
   *
   * @param path path to extract from
   * @param boundary boundary to search in a path
   * @param addTaskIdCompositeName if true will add task-id to the object name
   * @return new object name
   * @throws IOException if object name is missing
   */
  private String getObjectName(Path fullPath, String boundary,
      boolean addTaskIdCompositeName) throws IOException {
    String path = fullPath.toString();
    String noPrefix = path.substring(hostNameScheme.length());
    int npIdx = noPrefix.indexOf(boundary);
    String objectName = "";
    if (npIdx >= 0) {
      if (npIdx == 0 || npIdx == 1 && noPrefix.startsWith("/")) {
        //no object name present
        //schema://tone1.lvm/_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        //schema://tone1.lvm_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        throw new IOException("Object name is missing");
      } else {
        //path matches pattern in javadoc
        objectName = noPrefix.substring(0, npIdx - 1);
        if (addTaskIdCompositeName) {
          String taskAttempt = Utils.extractTaskID(path);
          String objName = fullPath.getName();
          if (taskAttempt != null && !objName.startsWith(HADOOP_ATTEMPT)) {
            objName = fullPath.getName() + "-" + taskAttempt;
          }
          objectName = objectName + "/" + objName;
        }
      }
      return objectName;
    }
    return noPrefix;
  }

  /**
   * Get object name with data root
   *
   * @param fullPath
   * @param boundary
   * @param addTaskIdCompositeName
   * @return composite of data root and object name
   * @throws IOException if object name is missing
   */
  private String getObjectNameRoot(Path fullPath, String boundary,
      boolean addTaskIdCompositeName) throws IOException {
    return storageClient.getDataRoot() + "/" + getObjectName(fullPath,
        boundary, addTaskIdCompositeName);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    LOG.debug("Glob status: {}", pathPattern.toString());
    return new ObjectStoreGlobber(this, pathPattern, DEFAULT_FILTER).glob();
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    LOG.debug("Glob status {} with path filter {}",pathPattern.toString(), filter.toString());
    return new ObjectStoreGlobber(this, pathPattern, filter).glob();
  }

  /**
   * {@inheritDoc}
   *
   * @return path to the working directory
   */
  @Override
  public Path getWorkingDirectory() {
    return storageClient.getWorkingDirectory();
  }

  /**
   * Default Path filter
   */
  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  };

}
