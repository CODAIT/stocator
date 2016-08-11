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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.httpclient.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.exception.ConnectionClosedException;

/**
 * Wraps the SwiftInputStream
 */
public class SwiftInputStreamWrapper extends InputStream {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftInputStreamWrapper.class);
  /*
   * HTTP method
   */
  private HttpMethod method;
  /*
   * Identify closed stream
   */
  private volatile boolean closed;
  /*
   * No more data to read
   */
  private volatile boolean finishReading;
  /*
   * Input stream
   */
  private InputStream inStream;

  /**
   * Constructor
   *
   * @param methodT HTTP method
   * @throws IOException if something went wrong
   */
  public SwiftInputStreamWrapper(HttpMethod methodT) throws IOException {
    method = methodT;
    try {
      inStream = method.getResponseBodyAsStream();
    } catch (IOException e) {
      inStream = new ByteArrayInputStream(new byte[] {});
      LOG.error(e.getMessage());
      throw closeAndThrow(e);
    }
  }

  @Override
  public void close() throws IOException {
    innerClose();
  }

  /**
   * Inner close method
   *
   * @return true if closed successfully
   * @throws IOException if close failed
   */
  private synchronized boolean innerClose() throws IOException {
    if (!closed) {
      try {
        if (method != null) {
          if (!finishReading) {
            method.abort();
          }
          method.releaseConnection();
        }
        if (inStream != null) {
          inStream.close();
        }
        return true;
      } finally {
        closed = true;
        finishReading = true;
      }
    } else {
      return false;
    }
  }

  /**
   * Close connection and throw an exception
   *
   * @param ex the exception
   * @return new exception
   */
  private IOException closeAndThrow(IOException ex) {
    try {
      innerClose();
    } catch (IOException ioe) {
      LOG.error(ioe.getMessage());
      if (ex == null) {
        ex = ioe;
      }
    }
    return ex;
  }

  /**
   * Check if connection is closed or input stream is null
   *
   * @throws ConnectionClosedException if connection is closed
   */
  private synchronized void verifyClosed() throws ConnectionClosedException {
    if (closed || inStream == null) {
      throw new ConnectionClosedException("Connection is closed");
    }
  }

  /**
   * Check if we haven't already finished reading
   *
   * @throws IOException if we have already finished
   */
  private synchronized void verifyNotFinished() throws IOException {
    if (finishReading) {
      LOG.debug("Already finished reading");
      throw closeAndThrow(new IOException("No more to read"));
    }
  }

  @Override
  public int available() throws IOException {
    verifyClosed();
    try {
      return inStream.available();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw closeAndThrow(e);
    }
  }

  @Override
  public int read() throws IOException {
    verifyNotFinished();
    verifyClosed();
    int read = 0;
    try {
      read = inStream.read();
    } catch (EOFException e) {
      LOG.error(e.getMessage());
      read = -1;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw closeAndThrow(e);
    }
    if (read < 0) {
      finishReading = true;
      LOG.trace("No more left to read");
      innerClose();
      read =  0;
    }
    return read;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int read;
    try {
      verifyNotFinished();
      verifyClosed();
    } catch (IOException e) {
      b = new byte[0];
      throw e;
    }
    try {
      read = inStream.read(b, off, len);
    } catch (EOFException e) {
      LOG.error(e.getMessage());
      read = -1;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw closeAndThrow(e);
    }
    if (read < 0) {
      finishReading = true;
      LOG.trace("No more data to read");
      innerClose();
      b = new byte[0];
      read = 0;
    }
    return read;
  }

}
