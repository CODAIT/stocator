/**
 * (C) Copyright IBM Corp. 2015, 2016
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;

/**
 * Internal object store driver interface
 * Each object back-end driver should implement this interface
 * Currently interface is based on the Swift API
 * Need to be adapted to cover more object stores, like Amazon S3
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
   * @return data root URI
   */
  public String getDataRoot();

  /**
   * Meta data of the object
   * @param hostName URL to host
   * @param path path to the object
   * @return FileStatus with the object info
   * @throws IOException if connection error
   * @throws FileNotFoundException if path not found
   */
  public FileStatus getObjectMetadata(String hostName,
                                      Path path) throws IOException, FileNotFoundException;

  /**
   * Verify if object exists
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
   * @throws IOException if connection error
   * @return FSDataInputStream to the object
   */
  public FSDataInputStream getObject(String hostName, Path path) throws IOException;

  /**
   * List container
   * @param hostName URL to host
   * @param path path to the object
   * @return arrays of FileStatus
   * @throws IOException if connection error
   */
  public FileStatus[] listContainer(String hostName,
                                    Path path) throws IOException;

  /**
   * Create object. Return output stream
   * @param objName name of the object
   * @param contentType content type
   * @param statistics file write stats
   * @return FSDataOutputStream
   * @throws IOException connection error
   */
  public FSDataOutputStream createObject(String objName, String contentType,
                                         Statistics statistics) throws IOException;

  /**
   * Get driver schema
   * @return String schema of the object driver
   */
  public String getScheme();

  /**
   * Delete an object
   *
   * @param hostName URL to host
   * @param path path to the object
   * @throws IOException object does not delete successfully
   * @return Boolean if the delete was successful or not
   */
  public boolean delete(String hostName, Path path, boolean recursive) throws IOException;

}
