/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.swift;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.exception.ConnectionClosedException;
import com.ibm.stocator.fs.swift.auth.JossAccount;

/**
 * Swift input stream
 */
class SwiftInputStream extends FSInputStream {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftInputStream.class);
  /*
   * Buffer size
   */
  private final long bufferSize;
  /*
   * Swift API client
   */
  private JossAccount mJossAccount;
  /*
   * Swift input stream wrapper
   */
  private SwiftInputStreamWrapper httpStream;
  /*
   * Data patch
   */
  private Path path;
  /*
   * Position in the buffer
   */
  private long pos = 0;
  /*
   * content length
   */
  private long contentLength = -1;
  /*
   * range diff
   */
  private long rangeOffset = 0;

  /**
   * Constructor
   *
   * @param apiClientT Swift API client
   * @param pathT data patch
   * @param bufferSizeT buffer size
   * @throws IOException if something went wrong
   */
  public SwiftInputStream(JossAccount jossAccountT, Path pathT,
      long bufferSizeT) throws IOException {
    mJossAccount = jossAccountT;
    path = pathT;
    bufferSize = bufferSizeT;
    SwiftGETResponse response = SwiftAPIDirect.getObject(path, mJossAccount);
    httpStream = response.getStreamWrapper();
  }

  /**
   * Get to new position
   *
   * @param diff how much to move
   */
  private synchronized void movePosition(int diff) {
    pos += diff;
    rangeOffset += diff;
  }

  /**
   * Update beginning of the buffer
   *
   * @param seekPos new position
   * @param contentLengthT content length
   */
  private synchronized void updateStartOfBufferPosition(long seekPos,
      long contentLengthT) {
    pos = seekPos;
    rangeOffset = 0;
    contentLength = contentLengthT;
    LOG.trace("Move: pos={}; bufferOffset={}; contentLength={}", pos,
        rangeOffset, contentLength);
  }

  @Override
  public synchronized int read() throws IOException {
    isConnectionOpen();
    int result = -1;
    try {
      result = httpStream.read();
    } catch (IOException e) {
      LOG.debug("IOException in reading {}" + path);
      LOG.debug(e.getMessage());
      if (reopenBuffer()) {
        LOG.debug("Reopen successfull");
        result = httpStream.read();
      }
    }
    if (result != -1) {
      movePosition(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    LOG.trace("{}: read from {} length {}", path.toString(), off, len);
    int result = -1;
    try {
      isConnectionOpen();
      result = httpStream.read(b, off, len);
    } catch (ConnectionClosedException e) {
      LOG.warn("Connection was closed during reading {}, try to reopen", path);
      LOG.warn(e.getMessage());
      return result;
    } catch (IOException e) {
      LOG.warn("IOException during reading {}, try to reopen", path);
      LOG.warn(e.getMessage());
      if (reopenBuffer()) {
        result = httpStream.read(b, off, len);
      }
    }
    if (result > 0) {
      movePosition(result);
    }
    return result;
  }

  /**
   * Attempt to reopen the buffer
   *
   * @return true on success
   * @throws IOException if failed to reopen
   */
  private boolean reopenBuffer() throws IOException {
    close();
    boolean success = false;
    try {
      loadIntoBuffer(pos);
      success = true;
    } catch (EOFException eof) {
      LOG.debug(eof.getMessage());
    }
    return success;
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

  /**
   * Check if connection is open
   *
   * @throws ConnectionClosedException if closed
   */
  private void isConnectionOpen() throws ConnectionClosedException {
    if (httpStream == null) {
      throw new ConnectionClosedException("http stream is null");
    }
  }

  /**
   * Pass over bytes
   *
   * @param bytes
   * @return
   * @throws IOException
   */
  private int chompBytes(long bytes) throws IOException {
    int count = 0;
    if (httpStream != null) {
      int result;
      for (long i = 0; i < bytes; i++) {
        result = httpStream.read();
        if (result < 0) {
          throw new IOException("Error while chomping");
        }
        count++;
        movePosition(1);
      }
    }
    return count;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    long offset = targetPos - pos;
    LOG.trace("{} : seek to {} from {}; offset {} ", path.toString(), targetPos, pos, offset);
    if (offset == 0) {
      return;
    }
    if (offset < 0) {
      LOG.debug("negative seek");
    } else if ((rangeOffset + offset < bufferSize)) {
      try {
        chompBytes(offset);
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
      if (targetPos - pos == 0) {
        return;
      }
    } else {
      LOG.debug("Seek is larger then buffer size " + bufferSize);
    }
    close();
    loadIntoBuffer(targetPos);
  }

  /**
   * Load data into the buffer
   *
   * @param targetPos offset
   * @throws IOException if something went wrong
   */
  private void loadIntoBuffer(long targetPos) throws IOException {
    long length = targetPos + bufferSize;
    LOG.debug("Reading {} bytes starting at {}", length, targetPos);
    SwiftGETResponse response = SwiftAPIDirect.getObject(path,
        mJossAccount, targetPos, targetPos + length - 1);
    httpStream = response.getStreamWrapper();
    updateStartOfBufferPosition(targetPos, response.getResponseSize());
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

}
