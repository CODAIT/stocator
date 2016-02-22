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

/**
 * Swift object store driver implementation
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
   * Host name with schema, e.g. swift://container.conf-entry/
   */
  private String hostNameScheme;

  @Override
  public String getScheme() {
    return storageClient.getScheme();
  }

  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);
    LOG.debug("Initialize: {}", fsuri.toString());
    if (!conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
      throw new IOException("mapreduce.fileoutputcommitter.marksuccessfuljobs should be enabled");
    }
    setConf(conf);
    String nameSpace = fsuri.toString().substring(0, fsuri.toString().indexOf("://"));
    if (storageClient == null) {
      storageClient = ObjectStoreVisitor.getStoreClient(nameSpace, fsuri, conf);
      hostNameScheme = storageClient.getScheme() + "://"  + Utils.getHost(fsuri) + "/";
      LOG.debug("ObjectStoreFileSystem has been initialized");
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
    LOG.debug("Check path: {}", path.toString());
  }

  /**
   * Check if the object exists in Swift.
   */
  @Override
  public boolean exists(Path f) throws IOException {
    LOG.debug("exists {}", f.toString());
    return storageClient.exists(hostNameScheme, f);
  }

  /**
   * There is no "directories" in the object store
   * The general structure is <container</<object>
   * <object> may contain nested structure
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

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    LOG.debug("Create method: {}", f.toString());
    String objNameModified = "";
    if (f.getName().equals(Constants.HADOOP_SUCCESS)) {
      String objectName = storageClient.getDataRoot() + "/"
          + getObjectName(f.toString(), Constants.HADOOP_SUCCESS);
      LOG.debug("Going to create {}", objectName);
      FSDataOutputStream outStream = storageClient.createObject(objectName,
          "application/directory", statistics);
      outStream.close();
      objNameModified = storageClient.getDataRoot() + "/"
          + getObjectName(f.toString(), Constants.HADOOP_TEMPORARY);
    } else {
      objNameModified = storageClient.getDataRoot() + "/"
          + getObjectName(f.toString(), Constants.HADOOP_TEMPORARY)
          + "/" + f.getName();
      LOG.debug("Tranformed to: {}", objNameModified);
    }
    FSDataOutputStream outStream = storageClient.createObject(objNameModified,
        "binary/octet-stream", statistics);
    getConf().set(f.toString(), "create");
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
    LOG.trace("rename from {} to {}", src.toString(), dst.toString());
    return true;
  }

  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("delete method: {}. recursive {}", f.toString(), recursive);
    return storageClient.delete(hostNameScheme, f, recursive);
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
    if (f.toString().contains("_temporary")) {
      return res;
    }
    return storageClient.listContainer(hostNameScheme, f);
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

  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    FileStatus[] res = {};
    if (f.toString().contains("_temporary")) {
      return res;
    }
    return storageClient.listContainer(hostNameScheme, f);
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

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    LOG.debug("mkdirs: {}", f.toString());
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
  public long getDefaultBlockSize() {
    return super.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    LOG.trace("Get default block size for: {}", f.toString());
    return super.getDefaultBlockSize(f);
  }

  /**
   * Extract object name from path. For example:
   * swift2d://tone1.lvm/aa/bb/cc/one3.txt/_temporary/0/_temporary/
   * attempt_201610052038_0001_m_000007_15/part-00007 will extract get
   * aa/bb/cc/one3.txt
   *
   * @param path
   * @return new object name
   */
  private String getObjectName(String path, String boundary) {
    LOG.trace("Extract object name from: {} with boundary {}", path, boundary);
    String noPrefix = path.substring(hostNameScheme.length());
    int pIdx = path.indexOf(boundary);
    String objectName = "";
    if (pIdx > 0) {
      int npIdx = noPrefix.indexOf(boundary);
      if (npIdx == 0 && noPrefix.contains("/")) {
        //swift2d://tone1.lvm/_temporary/0/_temporary/attempt_201610038_0001_m_000007_15/part-0007
        //noPrefix == _temporary/0/_temporary/attempt_201610052038_0001_m_000007_15/part-00007
        //no path in container reverse search for separator,
        //use the part name (e.g. part-00007) as object name
        objectName = noPrefix.substring(noPrefix.lastIndexOf("/") + 1, noPrefix.length());
      } else if (npIdx == 0) {
        //swift2d://tone1.lvm/_SUCCESS
        //noPrefix == _SUCCESS
        objectName = noPrefix;
      } else {
        //path matches pattern in javadoc
        objectName = noPrefix.substring(0, npIdx - 1);
      }
      LOG.debug("getObjectName - objectName: {}", objectName);
      return objectName;
    }
    return noPrefix;
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    LOG.debug("Glob status: {}", pathPattern.toString());
    return super.globStatus(pathPattern);
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    LOG.debug("Glob status {} with path filter {}",pathPattern.toString(), filter.toString());
    return super.globStatus(pathPattern, filter);
  }

  @Override
  public Path getWorkingDirectory() {
    return null;
  }
}
