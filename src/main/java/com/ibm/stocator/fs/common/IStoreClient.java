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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import com.ibm.stocator.fs.common.exception.ConfigurationParseException;

/**
 * Internal object store driver interface
 * Each object back-end driver should implement this interface
 */
public interface IStoreClient {

  /**
   * Block size of the object
   * Used by Hadoop to create data partition
   * @return block size
   */
  public long getBlockSize();

  /**
   * Data root URI
   *
   * @return data root URI
   */
  public String getDataRoot();

  /**
   * Meta data of the object
   *
   * @param hostName URL to host
   * @param path path to the object
   * @param msg identifier
   * @return FileStatus with the object info
   * @throws IOException if connection error
   * @throws FileNotFoundException if path not found
   */
  public FileStatus getFileStatus(String hostName,
      Path path, String msg) throws IOException, FileNotFoundException;

  /**
   * Verify if object exists
   *
   * @param hostName URL to host
   * @param path path to the object
   * @return true if object exists
   * @throws IOException if connection error
   * @throws FileNotFoundException if path not found
   */
  public boolean exists(String hostName,
      Path path) throws IOException, FileNotFoundException;

  /**
   * Get object
   * Returns InputStream that can be used to read data in chunks
   *
   * @param hostName URL to host
   * @param path path to the object
   * @return FSDataInputStream to the object
   * @throws IOException if connection error
   */
  public FSDataInputStream getObject(String hostName, Path path) throws IOException;

  /**
   * List data root.
   * Responsible to clean / filter temporal results from the failed tasks.
   *
   * @param hostName URL to host
   * @param path path to the object
   * @param fullListing if true, return all the content, including 0 byte size objects
   * @param prefixBased if set to true, container will be listed with prefix based query
   * @return arrays of FileStatus
   * @throws IOException if connection error
   */
  /*
  public FileStatus[] list(String hostName, Path path, boolean fullListing,
      boolean prefixBased) throws IOException;
  */
  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param hostName hostname
   * @param path given path
   * @param fullListing if true, return all the content, including 0 byte size objects
   * @param prefixBased if set to true, container will be listed with prefix based query
   * @param isDirectory is direct Globber call
   * @param flatListing is flat listing
   * @param filter PathFilter filter
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] list(String hostName, Path path, boolean fullListing,
      boolean prefixBased, Boolean isDirectory,
      boolean flatListing, PathFilter filter) throws FileNotFoundException, IOException;

  /**
   * Create object. Return output stream
   *
   * @param objName name of the object
   * @param contentType content type
   * @param metadata the metadata to create with an object
   * @param statistics the statistics for this file system
   * @param overwrite if true, overwrite existing object
   * @return FSDataOutputStream
   * @throws IOException if connection error
   */
  public FSDataOutputStream createObject(String objName, String contentType,
      Map<String, String> metadata, Statistics statistics, boolean overwrite) throws IOException;

  /**
   * Get driver schema
   *
   * @return String schema of the object driver
   */
  public String getScheme();

  /**
   * Delete an object
   *
   * @param hostName URL to host
   * @param path path to the object
   * @param recursive recursive flag
   * @return if the path was deleted
   * @throws IOException if connection error
   */
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException;

  /**
   * Some drivers requires local temporary directory
   *
   * @return working directory
   */
  public Path getWorkingDirectory();

  /**
   * @param newDir new working directory
   */
  public void setWorkingDirectory(Path newDir);

  /**
   * Return authenticated access URI
   * @return access URI
   * @throws IOException if something went wrong
   */
  public URI getAccessURI() throws IOException;

  /**
   * Rename operation
   *
   * @param hostName URL to host
   * @param srcPath source path
   * @param dstPath destination path
   * @return true if successful
   * @throws IOException if something went wrong
   */
  public boolean rename(String hostName, String srcPath, String dstPath) throws IOException;

  /**
   * Contains the logic for the driver initialization
   * @param scheme schema
   * @throws ConfigurationParseException if failed to parse the configuration
   * @throws IOException otherwise
   */
  public void initiate(String scheme) throws IOException, ConfigurationParseException;

  /**
   * Set stocator path
   * @param sp Stocator path
   */
  public void setStocatorPath(StocatorPath sp);

  /**
   * @return is flat listing
   */
  public boolean isFlatListing();

  /**
   * @param stat Statistics
   */
  public void setStatistics(Statistics stat);

  /**
   * @param path input path to qualify
   * @return qualified path
   */
  public Path qualify(Path path);

}
