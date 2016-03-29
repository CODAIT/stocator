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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private int totalBytesWritten = 0;
  private static final int MAX_PARTITION_SIZE = 512 * 1024;
  private int partitionCount = 0;

  /**
   * Default constructor
   *
   * @param httpCon URL connection
   * @throws IOException if failed to connect
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
    LOG.info("write1 called");
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (totalBytesWritten + len >= MAX_PARTITION_SIZE) {
      splitFileUpload();
      totalBytesWritten = 0;
    }
    mOutputStream.write(b, off, len);
    totalBytesWritten += len;
    //LOG.info("{} bytes written", totalBytesWritten);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOutputStream.write(b);
    LOG.info("writes2 called");
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
    LOG.info("{} bytes written", totalBytesWritten);
    InputStream is = null;
    try {
      // Status 400 and up should be read from error stream
      // Expecting here 201 Create or 202 Accepted
      if (mHttpCon.getResponseCode() >= 400) {
        is = mHttpCon.getErrorStream();
        LOG.info("Error code: {}", mHttpCon.getResponseCode());
      } else {
        is = mHttpCon.getInputStream();
        LOG.info("No error");
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

  private void splitFileUpload() throws IOException {
    mOutputStream.close();
    URL oldURL = mHttpCon.getURL();
    String objName = oldURL.getPath() + Integer.toString(++partitionCount);
    URL newURL = new URL(oldURL.getProtocol() + "://" + oldURL.getAuthority() + objName);

    LOG.info("New URL path: {} {} {}", newURL.getProtocol(),
            newURL.getAuthority(), objName);

    try {
      mHttpCon.disconnect();
      HttpURLConnection newConn = (HttpURLConnection) newURL.openConnection();

      newConn.setDoInput(true);
      newConn.setRequestMethod("PUT");

      newConn.setReadTimeout(READ_TIMEOUT);
      newConn.setChunkedStreamingMode(STREAMING_CHUNK);

      Set<Map.Entry<String, List<String>>> properties = mHttpCon.getRequestProperties().entrySet();
      for (Map.Entry<String, List<String>> property : properties) {
        for (String value : property.getValue()) {
          LOG.info("Key: {} Value: {}", property.getKey(), value);
          if (!(property.getKey().contains("POST") || property.getKey().contains("PUT"))) {
            newConn.setRequestProperty(property.getKey(), value);
            LOG.info("Setting requests key: {} value: {}", property.getKey(), value);
          }
        }
      }
      newConn.setDoOutput(true);

      LOG.info("Settings configured");

      Set<Map.Entry<String, List<String>>> debugProps = newConn.getRequestProperties().entrySet();
      for (Map.Entry<String, List<String>> property : debugProps) {
        for (String value : property.getValue()) {
          LOG.info("Key: {} set to value: {}", property.getKey(), value);
        }
      }

      mOutputStream = newConn.getOutputStream();
      mHttpCon = newConn;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

}
