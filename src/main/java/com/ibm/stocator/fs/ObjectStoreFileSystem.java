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
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.UrlEscapers;
import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.common.ObjectStoreGlobber;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.StocatorPath;
import com.ibm.stocator.fs.common.ExtendedFileSystem;

import static com.ibm.stocator.fs.common.Constants.HADOOP_ATTEMPT;
import static com.ibm.stocator.fs.common.Constants.HADOOP_TEMPORARY;
import static com.ibm.stocator.fs.common.Constants.OUTPUT_COMMITTER_TYPE;
import static com.ibm.stocator.fs.common.Constants.DEFAULT_FOUTPUTCOMMITTER_V1;
import static com.ibm.stocator.fs.common.Constants.FS_STOCATOR_GLOB_BRACKET_SUPPORT;
import static com.ibm.stocator.fs.common.Constants.FS_STOCATOR_GLOB_BRACKET_SUPPORT_DEFAULT;

/**
 * Object store driver implementation
 * Based on the Hadoop FileSystem interface
 *
 */
public class ObjectStoreFileSystem extends ExtendedFileSystem {

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

  /*
   * full URL to the data path
   */
  private URI uri;
  private StocatorPath stocatorPath;
  private boolean bracketGlobSupport;

  @Override
  public String getScheme() {
    return storageClient.getScheme();
  }

  @Override
  public void initialize(URI fsuri, Configuration conf) throws IOException {
    super.initialize(fsuri, conf);
    LOG.trace("Initialize for {}", fsuri);
    if (!conf.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
      throw new IOException("mapreduce.fileoutputcommitter.marksuccessfuljobs should be enabled");
    }
    final String escapedAuthority = UrlEscapers
            .urlPathSegmentEscaper()
            .escape(fsuri.getAuthority());
    uri = URI.create(fsuri.getScheme() + "://" + escapedAuthority);
    setConf(conf);
    String committerType = conf.get(OUTPUT_COMMITTER_TYPE, DEFAULT_FOUTPUTCOMMITTER_V1);
    bracketGlobSupport = conf.getBoolean(FS_STOCATOR_GLOB_BRACKET_SUPPORT,
        FS_STOCATOR_GLOB_BRACKET_SUPPORT_DEFAULT.equals("true"));
    if (storageClient == null) {
      storageClient = ObjectStoreVisitor.getStoreClient(fsuri, conf);
      if (Utils.validSchema(fsuri.toString())) {
        hostNameScheme = storageClient.getScheme() + "://"  + Utils.getHost(fsuri) + "/";
      } else {
        LOG.debug("Non valid schema for {}", fsuri.toString());
        String accessURL = Utils.extractAccessURL(fsuri.toString(), storageClient.getScheme());
        LOG.debug("Non valid schema. Access url {}", accessURL);
        String dataRoot = Utils.extractDataRoot(fsuri.toString(),
            accessURL);
        if (dataRoot.isEmpty()) {
          hostNameScheme = accessURL + "/";
        } else {
          hostNameScheme = accessURL + "/" + dataRoot + "/";
        }
      }
      stocatorPath = new StocatorPath(committerType, conf, hostNameScheme);
      storageClient.setStocatorPath(stocatorPath);
      storageClient.setStatistics(statistics);
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Check path should check the validity of the path. Skipped at this point.
   */
  @Override
  protected void checkPath(Path path) {
    LOG.trace("Check path: {}. Not implemented", path.toString());
  }

  /**
   * There is no "directories" in the object store
   * The general structure is "dataroot/object"
   * and "object" may contain nested structure
   *
   * qualify path if doesn't of the form scheme://bucket
   */
  @Override
  public boolean isDirectory(final Path f) throws IOException {
    if (stocatorPath.isTemporaryPath(f)) {
      return false;
    }
    final Path path = storageClient.qualify(f);
    try {
      FileStatus fileStatus = getFileStatus(path);
      LOG.debug("is directory: {} : {}", path.toString(), fileStatus.isDirectory());
      return fileStatus.isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * qualify path if doesn't of the form scheme://bucket
   */
  @Override
  public boolean isFile(Path f) throws IOException {
    if (stocatorPath.isTemporaryPath(f)) {
      return true;
    }
    final Path path = storageClient.qualify(f);
    try {
      FileStatus fileStatus = getFileStatus(path);
      LOG.debug("is file: {}" + path.toString() + " " + fileStatus.isFile());
      return fileStatus.isFile();
    } catch (FileNotFoundException e) {
      return false;
    }
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
  public FSDataInputStream open(Path path) throws IOException {
    return open(path, 0);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    LOG.debug("open: {} with buffer size {}", path.toString(), bufferSize);
    Path qualifiedPath = storageClient.qualify(path);
    return storageClient.getObject(hostNameScheme, qualifiedPath);
  }

  /**
   * {@inheritDoc}
   * create path of the form dataroot/objectname
   * Each object name is modified to contain task-id prefix.
   * Thus for example, create
   * dataroot/objectname/_temporary/0/_temporary/attempt_201603131849_0000_m_000019_0/
   * part-r-00019-a08dcbab-8a34-4d80-a51c-368a71db90aa.csv
   * will be transformed to
   * PUT dataroot/objectname
   * /part-r-00019-a08dcbab-8a34-4d80-a51c-368a71db90aa.csv-attempt_201603131849_0000_m_000019_0
   *
   */
  public FSDataOutputStream  create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    LOG.debug("Create: {}, overwrite is: {}", f.toString(), overwrite);
    validateBracketSupport(f.toString());
    final Path path = storageClient.qualify(f);
    final String objNameModified;
    // check if request is dataroot/objectname/_SUCCESS
    if (path.getName().equals(Constants.HADOOP_SUCCESS)) {
      // no need to add attempt id to the _SUCCESS
      objNameModified =  stocatorPath.extractFinalKeyFromTemporaryPath(path, false,
          storageClient.getDataRoot(), true);
    } else {
      // add attempt id to the final name
      objNameModified = stocatorPath.extractFinalKeyFromTemporaryPath(path, true,
          storageClient.getDataRoot(), true);
    }
    return storageClient.createObject(objNameModified,
        "application/octet-stream", null, statistics);
  }

  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Append is not supported in the object storage");
  }

  /**
   * {@inheritDoc}
   * We don't need rename on temporary objects, since objects are already were created with real
   * names.
   */
  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    LOG.debug("rename from {} to {}", src, dst);
    if (src == null || dst == null) {
      LOG.debug("Source path and dest path can not be null");
      return false;
    }
    if (stocatorPath.isTemporaryPath(src)) {
      return true;
    }
    LOG.debug("Checking if source exists {}", src);
    if (!exists(src)) {
      LOG.debug("Source {} does not exists. Exit", src);
      return false;
    }
    final Path srcPath = storageClient.qualify(src);
    final Path dstPath = storageClient.qualify(dst);
    return storageClient.rename(hostNameScheme, srcPath.toString(), dstPath.toString());
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    LOG.debug("About to delete {}", f.toString());
    if (stocatorPath.isTemporaryPath(f)) {
      return true;
    }
    final Path path = storageClient.qualify(f);
    if (path.toString().equals(hostNameScheme)) {
      LOG.warn("{} {}", path.toString(), "Cannot delete root path");
      return true;
    }
    LOG.trace("Delete: qualify input path {} to {}", f, path);
    // this will strip temp structure if present and generate real
    // object name without hadoop_attempt, _temporary, etc.
    String objNameModified = stocatorPath.extractFinalKeyFromTemporaryPath(path, true,
        storageClient.getDataRoot(), false);
    // create new full path again, this time without hadoop_attempt, _temporary, etc.
    Path reducedPath = storageClient.qualify(new Path(hostNameScheme, objNameModified));
    LOG.debug("delete: reduced path {} recursive {}. modifed name {}, hostname {}",
        reducedPath.toString(), recursive, objNameModified, hostNameScheme);

    boolean deleteMainEntry = true;
    if (f.getName().startsWith(HADOOP_ATTEMPT)) {
      FileStatus[] fsList = storageClient.list(hostNameScheme, reducedPath.getParent(), true, true,
          null, false, null);
      if (fsList.length > 0) {
        for (FileStatus fs: fsList) {
          if (fs.getPath().getName().endsWith(path.getName())) {
            storageClient.delete(hostNameScheme, fs.getPath(), recursive);
          }
        }
      }
    } else {
      LOG.debug("delete: {} is not temporary path and not starts with HADOOP_ATTEMPT",
          f.toString());
      FileStatus[] fsList = storageClient.list(hostNameScheme, reducedPath, true, true,
          Boolean.TRUE, recursive, null);
      if (fsList.length > 0) {
        for (FileStatus fs: fsList) {
          LOG.debug("Delete candidate {} reduced path {}", fs.getPath().toString(),
              reducedPath.toString());
          String pathToDelete = reducedPath.toString();
          if (!pathToDelete.endsWith("/")) {
            pathToDelete = pathToDelete + "/";
          }
          LOG.debug("Delete candidate {} ", fs.getPath().toString(), pathToDelete);
          if (fs.getPath().toString().equals(path.toString())
              || fs.getPath().toString().startsWith(pathToDelete)) {
            LOG.debug("Delete {} from the list of {}", fs.getPath(), reducedPath);
            storageClient.delete(hostNameScheme, fs.getPath(), recursive);
            if (fs.getPath().toString().equals(path.toString())) {
              deleteMainEntry = false;
            }
          }
        }
      }
    }
    if (!hostNameScheme.equals(reducedPath.toString()) && deleteMainEntry) {
      LOG.debug("*** Delete main entry {}", reducedPath);
      storageClient.delete(hostNameScheme, reducedPath, recursive);
    }
    return true;
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter)
          throws FileNotFoundException, IOException {
    return listStatus(f, filter, false, null);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    LOG.debug("List status of {}", f.toString());
    return listStatus(f, null);
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter, boolean prefixBased,
      Boolean isDirectory) throws FileNotFoundException, IOException {
    LOG.debug("listStatus: {},  prefix based {}. Globber is directory status {}",
        f.toString(), prefixBased, isDirectory);
    if (stocatorPath.isTemporaryPath(f)) {
      LOG.debug("{} temporary. Return empty result", f);
      return new FileStatus[]{};
    }
    FileStatus fileStatus = null;
    if (isDirectory == null) {
      try {
        LOG.trace("listStatus: internal get status-start for {}", f);
        fileStatus = getFileStatus(f);
        if (fileStatus != null) {
          LOG.trace("listStatus: internal get status-finish for {}. Directory {}", f.toString(),
              fileStatus.isDirectory());
          if (fileStatus.isDirectory()) {
            isDirectory = Boolean.TRUE;
          }
        }
      } catch (FileNotFoundException e) {
        if (!prefixBased) {
          throw e;
        }
      }
    }
    // we need this,since ObjectStoreGlobber may send prefix
    // container/objectprefix* and objectprefix is not exists as an object or
    // pseudo directory
    if (fileStatus != null && !(prefixBased || (isDirectory != null && isDirectory))) {
      LOG.debug("listStatus: {} is not a directory, but a file. Return single element",
          f.toString());
      FileStatus[] stats = new FileStatus[1];
      stats[0] = fileStatus;
      return stats;
    }
    LOG.debug("listStatus: {} -not found. Prefix based listing set to {}. Perform list",
        f.toString(), prefixBased);
    Path path = storageClient.qualify(f);
    final FileStatus[] listing;
    if (!storageClient.isFlatListing()) {
      LOG.trace("Using hadoop list style, non flat list {}", f);
      listing =  storageClient.list(hostNameScheme, path, false, prefixBased,
          isDirectory, storageClient.isFlatListing(), filter);
    } else {
      LOG.trace("Using stocator list style, flat list {}", f);
      listing = storageClient.list(hostNameScheme, path, false, prefixBased, isDirectory,
          storageClient.isFlatListing(), filter);
    }
    LOG.debug("listStatus: {} completed. return {} results", path.toString(),
        listing.length);
    return listing;
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
    storageClient.setWorkingDirectory(new_dir);
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
   */
  @Override
  public boolean mkdirs(Path f) throws IOException, FileAlreadyExistsException {
    LOG.debug("mkdirs: {}", f.toString());
    validateBracketSupport(f.toString());
    if (stocatorPath.isTemporaryPathTarget(f.getParent())) {
      final Path path = storageClient.qualify(f);
      String objNameModified = stocatorPath.extractFinalKeyFromTemporaryPath(path,true,
          storageClient.getDataRoot(), true);
      final Path pathToObj = new Path(objNameModified);
      LOG.trace("mkdirs {} modified name", objNameModified);
      // make sure there is no overwrite of existing data
      // if we here, means getfilestatus() returned false and
      // fileoutputcomitter is about to create job temp folder
      // in this case there is no need to check wether base directory exists
      // as it was already checked in getfilestatus() method
      /*
      try {
        String directoryToExpect = stocatorPath.getBaseDirectory(f.toString());
        FileStatus fileStatus = getFileStatus(new Path(directoryToExpect));
        if (fileStatus != null) {
          LOG.debug("mkdirs found {} as exists. Directory : {}", directoryToExpect,
              fileStatus.isDirectory());
          throw new FileAlreadyExistsException("mkdir on existing directory " + directoryToExpect);
        }
      } catch (FileNotFoundException e) {
        LOG.debug("mkdirs {} - not exists. Proceed", pathToObj.getParent().toString());
      }
      */
      String plainObjName = pathToObj.getParent().toString();
      LOG.debug("Going to create identifier {}", plainObjName);
      final Map<String, String> metadata = new HashMap<>();
      metadata.put("Data-Origin", "stocator");
      FSDataOutputStream outStream = storageClient.createObject(plainObjName,
          Constants.APPLICATION_DIRECTORY, metadata, statistics);
      outStream.close();
    } else {
      LOG.debug("mkdirs on non temp object. Create {}", f.toString());
      String objName = stocatorPath.extractFinalKeyFromTemporaryPath(f, false,
          storageClient.getDataRoot(), true);
      if (!objName.endsWith("/")) {
        objName = objName + "/";
      }
      LOG.trace("mkdirs to create directory {}", objName);
      FSDataOutputStream outStream = storageClient.createObject(objName,
          Constants.APPLICATION_DIRECTORY, null, statistics);
      outStream.close();
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    /*
     * Issues might happen with Dataframes of the recent Spark versions
     * In some flows Spark aware of temporary files and request getFileStatus(tempFile)
     * Spark uses this information to update BasicWriteTaskStats
     * As example, Spark may call
     * getFileStatus('scheme://bucket.service/a/data.parquet/_temporary/0/
     *      _temporary/attempt_20181009100745_0001_m_000006_0/
     *      part-00006-87428114-b6c6-49fc-9b4c-2415da470115-c000.snappy.parquet')
     * As Stocator doesn't persists temporary files, internally this call should be mapped to
     * getFileStatus('scheme://bucket.service/a/data.parquet/
     *      part-00006-87428114-b6c6-49fc-9b4c-2415da470115-c000.snappy.parquet')
     * Notice:!
     * Object was written as
     * part-00006-87428114-b6c6-49fc-9b4c-2415da470115-c00
     *      -attempt_20181009100745_0001_m_000000_0.snappy.parquet
     * while Spark accesses
     * part-00006-87428114-b6c6-49fc-9b4c-2415da470115-c000.snappy.parquet
     *
     */
    LOG.debug("get file status: {}", f.toString());
    final Path path = storageClient.qualify(f);
    return storageClient.getFileStatus(hostNameScheme, path, "fileStatus");
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
    final long defaultBlockSize = super.getDefaultBlockSize(f);
    LOG.trace("Default block size for: {} is {}", f.toString(), defaultBlockSize);
    return defaultBlockSize;
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    LOG.debug("Glob status: {}", pathPattern.toString());
    return new ObjectStoreGlobber(this, pathPattern, DEFAULT_FILTER, bracketGlobSupport).glob();
  }

  @Override
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
    LOG.debug("Glob status {} with path filter {}",pathPattern.toString(), filter.toString());
    return new ObjectStoreGlobber(this, pathPattern, filter, bracketGlobSupport).glob();
  }

  @Override
  public boolean exists(Path f) throws IOException {
    LOG.trace("Object exists: {}", f);
    if (f.toString().contains(HADOOP_TEMPORARY)) {
      LOG.debug("Exists on temp object {}. Return false", f.toString());
      return false;
    }
    if (f.toString().contains("*")) {
      LOG.debug("Exists on object {}. Return false", f.toString());
      return false;
    }
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
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

  private void validateBracketSupport(String path) throws IOException {
    if (path != null && bracketGlobSupport && path.indexOf("{") > 0 && path.indexOf("}") > 0) {
      LOG.debug("Bracket glob support: {}. Path {} can not contain brackets",
          bracketGlobSupport, path);
      throw new IOException("Bracket glob support enabled. Path can not contain brackets " + path);
    }
  }

  @Override
  public String getHostnameScheme() {
    if (hostNameScheme.endsWith("/")) {
      return hostNameScheme.substring(0, hostNameScheme.length() - 1);
    }
    return hostNameScheme;
  }
}
