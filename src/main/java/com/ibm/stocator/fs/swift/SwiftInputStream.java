/**
 * (C) Copyright IBM Corp. 2010, 2016
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
import java.io.InputStream;

import org.javaswift.joss.headers.object.range.AbstractRange;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.model.StoredObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

class SwiftInputStream extends FSInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(SwiftInputStream.class);

  /*
   * File nativeStore instance
   */
  private final SwiftAPIClient nativeStore;

  /*
   * Data input stream
   */
  private InputStream httpStream;

  /*
   * Current position
   */
  private long pos = 0;

  /*
   * Reference to the stored object
   */
  private StoredObject storedObject;

  public SwiftInputStream(SwiftAPIClient storeNative, String hostName,
      Path path) throws IOException {
    nativeStore = storeNative;
    LOG.debug("init: {}", path.toString());
    String objectName = path.toString().substring(hostName.length());
    storedObject = nativeStore.getAccount().getContainer(nativeStore.getDataRoot())
        .getObject(objectName);
    if (!storedObject.exists()) {
      throw new FileNotFoundException(objectName + " is not exists");
    }
  }

  @Override
  public synchronized int read() throws IOException {
    if (httpStream == null) {
      // not sure we need it. need to re-check.
      seek(0);
    }
    int result = -1;
    result = httpStream.read();
    return result;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    int result = -1;
    if (httpStream == null) {
      // not sure we need it. need to re-check.
      seek(0);
    }
    result = httpStream.read(b, off, len);
    return result;
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (httpStream != null) {
        httpStream.close();
      }
    } finally {
      httpStream = null;
    }
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    LOG.debug("seek method to: {}, for {}", targetPos, storedObject.getName());
    DownloadInstructions instructions = new DownloadInstructions();
    AbstractRange range = new AbstractRange(targetPos, targetPos + nativeStore.getBlockSize()) {

      @Override
      public long getTo(int arg0) {
        return offset;
      }

      @Override
      public long getFrom(int arg0) {
        return length;
      }
    };
    instructions.setRange(range);
    httpStream = storedObject.downloadObjectAsInputStream(instructions);
    LOG.debug("Seek completed. Got http stream for: {}", storedObject.getName());
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  /**
   * multiple data sources not yet supported
   *
   * @param targetPos new target position
   * @return true if new data source set successfull
   * @throws IOException
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }
}
