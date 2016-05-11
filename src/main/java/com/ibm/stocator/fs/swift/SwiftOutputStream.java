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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  /*
   * Maximum size to be written to a Swift object before a new object is created.
   */
  private long maxSplitSize;
  private long totalBytesWritten = 0L;
  private int splitCount = 0;
  private static final int INT_BYTE_SIZE = 4;

  /**
   * Default constructor
   *
   * @param httpCon URL connection
   * @throws IOException if failed to connect
   */

  public SwiftOutputStream(HttpURLConnection httpCon, long maxObjectSize) throws IOException {
    try {
      httpCon.setDoInput(true);
      httpCon.setRequestProperty("Connection", "close");
      httpCon.setReadTimeout(READ_TIMEOUT);
      httpCon.setRequestProperty("Transfer-Encoding","chunked");
      httpCon.setDoOutput(true);
      httpCon.setChunkedStreamingMode(STREAMING_CHUNK);
      maxSplitSize = maxObjectSize;
      mOutputStream  = httpCon.getOutputStream();
      mHttpCon = httpCon;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (totalBytesWritten + INT_BYTE_SIZE >= maxSplitSize) {
      splitFileUpload();
      totalBytesWritten = 0;
    }
    mOutputStream.write(b);
    totalBytesWritten += INT_BYTE_SIZE;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (totalBytesWritten + len >= maxSplitSize) {
      splitFileUpload();
      totalBytesWritten = 0;
    }
    mOutputStream.write(b, off, len);
    totalBytesWritten += len;
  }

  @Override
  public void write(byte[] b) throws IOException {
    if (totalBytesWritten + b.length >= maxSplitSize) {
      splitFileUpload();
      totalBytesWritten = 0;
    }
    mOutputStream.write(b);
    totalBytesWritten += b.length;
  }

  @Override
  public void close() throws IOException {
    LOG.trace("{} bytes written", totalBytesWritten);
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

  /*
   * Closes the old stream and sets up a new stream
   */
  private void splitFileUpload() throws IOException {
    mOutputStream.close();
    URL oldURL = mHttpCon.getURL();
    String prevSplitName = oldURL.getPath();
    String currSplitName;
    if (splitCount == 0) {
      Pattern p = Pattern.compile("part-\\d\\d\\d\\d\\d-");
      Matcher m = p.matcher(prevSplitName);
      if (m.find()) {
        currSplitName = new StringBuilder(prevSplitName).insert(m.end(),
                "split-" + String.format("%05d", ++splitCount) + "-").toString();
      } else {
        currSplitName = new StringBuilder(prevSplitName).append("-split-"
                + String.format("%05d", ++splitCount)).toString();
      }
    } else {
      currSplitName = prevSplitName.replace("split-\\d\\d\\d\\d\\d",
              "split-" + String.format("%05d", ++splitCount));
    }
    URL newURL = new URL(oldURL.getProtocol() + "://" + oldURL.getAuthority() + currSplitName);
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
          if (!(property.getKey().contains("POST") || property.getKey().contains("PUT"))) {
            newConn.setRequestProperty(property.getKey(), value);
          }
        }
      }
      newConn.setDoOutput(true);
      mOutputStream = newConn.getOutputStream();
      mHttpCon = newConn;
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

}
