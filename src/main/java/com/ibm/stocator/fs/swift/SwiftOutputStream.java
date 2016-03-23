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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Wraps OutputStream
 * This class is not thread-safe
 *
 */
public class SwiftOutputStream extends OutputStream {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftOutputStream.class);
  /*
   * Streaming chunk size
   */
  private static final int STREAMING_CHUNK = 8 * 1024 * 1024;
  /*
   * Read time out
   */
  private static final int READ_TIMEOUT = 100 * 1000;
  /*
   * Output stream
   */
  private OutputStream mOutputStream;
  /*
   * HTTP connection object
   */
  private HttpURLConnection mHttpCon;

  /**
   * Default constructor
   *
   * @param httpCon
   * @throws IOException
   */
  public SwiftOutputStream(HttpURLConnection httpCon) throws IOException {
    try {
      httpCon.setDoInput(true);
      httpCon.setRequestProperty("Connection", "close");
      httpCon.setReadTimeout(READ_TIMEOUT);
      httpCon.setRequestProperty("Transfer-Encoding","chunked");
      httpCon.setDoOutput(true);
      httpCon.setChunkedStreamingMode(STREAMING_CHUNK);
      mOutputStream  = httpCon.getOutputStream();
      mHttpCon = httpCon;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
    InputStream is = null;
    try {
      // Status 400 and up should be read from error stream
      // Expecting here 201 Create or 202 Accepted
      if (mHttpCon.getResponseCode() >= 400) {
        is = mHttpCon.getErrorStream();
      } else {
        is = mHttpCon.getInputStream();
      }
      is.close();
    } catch (Exception e) {
      if (is != null) {
        is.close();
      }
      LOG.error(e.getMessage());
      throw e;
    }
    mHttpCon.disconnect();
  }

  @Override
  public void flush() throws IOException {
    mOutputStream.flush();
  }
}
