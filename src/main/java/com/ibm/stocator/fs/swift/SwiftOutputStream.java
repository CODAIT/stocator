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
import java.net.ProtocolException;
import java.net.URL;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.auth.JossAccount;

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

  /*
   * Access url
   */
  private URL mUrl;

  /**
   * Default constructor
   *
   * @param account Joss account object
   * @param url URL connection
   * @param contentType content type
   * @param metadata input metadata
   * @throws IOException if error
   */
  public SwiftOutputStream(JossAccount account, URL url, String contentType,
      Map<String, String> metadata) throws IOException {
    mUrl = url;
    HttpURLConnection httpCon = createConnection(account, url, contentType, metadata);
    try {
      LOG.debug("Going to obtain output stream for PUT {}", url.toString());
      mOutputStream  = httpCon.getOutputStream();
      LOG.debug("Output stream created for PUT {}", url.toString());
      mHttpCon = httpCon;
    } catch (ProtocolException e) {
      LOG.warn("Failed to connect to {}", url.toString());
      LOG.warn(e.getMessage());
      LOG.warn("Retry attempt for PUT {}. Re-authenticate", url.toString());
      account.authenticate();
      httpCon = createConnection(account, url, contentType, metadata);
      mOutputStream  = httpCon.getOutputStream();
      LOG.debug("Second attempt PUT {} successfull. Got output stream", url.toString());
      mHttpCon = httpCon;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  /**
   * Creates HTTP Connection
   *
   * @param account Joss Account
   * @param url URL to the object
   * @param contentType object content type
   * @param metadata user provided meta data
   * @return HttpURLConnection if success
   * @throws IOException if error
   */
  private HttpURLConnection createConnection(JossAccount account, URL url, String contentType,
      Map<String, String> metadata) throws IOException {
    LOG.debug("Create chunked HTTP connection for PUT {}. Read timeout {}. Streaming chunk {}",
        url.toString(), READ_TIMEOUT, STREAMING_CHUNK);
    HttpURLConnection newHttpCon = (HttpURLConnection) url.openConnection();
    newHttpCon.setDoOutput(true);
    newHttpCon.setRequestMethod("PUT");

    newHttpCon.addRequestProperty("X-Auth-Token",account.getAuthToken());
    newHttpCon.addRequestProperty("Content-Type", contentType);
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        newHttpCon.addRequestProperty("X-Object-Meta-" + entry.getKey(), entry.getValue());
      }
    }
    newHttpCon.setDoInput(true);
    newHttpCon.setRequestProperty("Connection", "close");
    newHttpCon.setReadTimeout(READ_TIMEOUT);
    newHttpCon.setRequestProperty("Transfer-Encoding","chunked");
    newHttpCon.setDoOutput(true);
    newHttpCon.setRequestProperty("Expect", "100-continue");
    newHttpCon.setChunkedStreamingMode(STREAMING_CHUNK);

    return newHttpCon;
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
    LOG.trace("Close the output stream for {}", mUrl.toString());
    mOutputStream.close();
    InputStream is = null;
    try {
      // Status 400 and up should be read from error stream
      // Expecting here 201 Create or 202 Accepted
      int responseCode = mHttpCon.getResponseCode();
      if (responseCode >= 400) {
        LOG.warn("{}, {}, {}", mUrl.toString(), responseCode, mHttpCon.getResponseMessage());
        is = mHttpCon.getErrorStream();
      } else {
        is = mHttpCon.getInputStream();
        LOG.debug("{}, {}, {}", mUrl.toString(), responseCode, mHttpCon.getResponseMessage());
      }
      if (responseCode == 401 || responseCode == 403 || responseCode == 407) {
        // special handling. HTTPconnection already closed stream.
        LOG.warn("{} : stream closed to {}", responseCode, mUrl.toString());
      }
      if (is != null) {
        is.close();
      }
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
