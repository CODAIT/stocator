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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
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
  //private HttpURLConnection mHttpCon;

  /*
   * Access url
   */
  private URL mUrl;

  /*
   * Client used
   */
  private HttpClient client;

  /*
   * Request
   */
  private HttpPut request;

  /*
   * Response
   */
  private HttpResponse response;
  private Thread writeThread;
  private long totalWritten;

  /**
   * Default constructor
   *
   * @param account JOSS account object
   * @param url URL connection
   * @param contentType content type
   * @param metadata input metadata
   * @throws IOException if error
   */
  public SwiftOutputStream(JossAccount account, URL url, String contentType,
      Map<String, String> metadata) throws IOException {
    mUrl = url;
    totalWritten = 0;
    //mOutputStream = new ByteArrayOutputStream();
    client = HttpClients.createDefault();
    request = new HttpPut(mUrl.toString());
    request.addHeader("X-Auth-Token", account.getAuthToken());
    request.addHeader("Content-Type", contentType);
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        request.addHeader("X-Object-Meta-" + entry.getKey(), entry.getValue());
      }
    }
    PipedOutputStream out = new PipedOutputStream();
    final PipedInputStream in = new PipedInputStream();
    out.connect(in);
    mOutputStream = out;
    writeThread = new Thread() {
      public void run() {
        //create your http request
        InputStreamEntity entity = new InputStreamEntity(in, -1);
        request.setEntity(entity);
        try {
          client.execute(request);
        } catch (IOException e) {
          System.out.println("IOException");
        }
      }
    };

/*
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
    }*/
  }

  private void startThread() {
    if (writeThread.getState().equals(Thread.State.NEW)) {
      writeThread.start();
    }
  }

  /**
   * Creates HTTP Connection
   *
   * @param account JOSS Account
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
    totalWritten = totalWritten + 1;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Write {} one byte. Total written {}", mUrl.toString(), totalWritten);
    }
    startThread();
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    totalWritten = totalWritten + len;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Write {} off {} len {}. Total {}", mUrl.toString(), off, len, totalWritten);
    }
    startThread();
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    totalWritten = totalWritten + b.length;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Write {} len {}. Total {}", mUrl.toString(), b.length, totalWritten);
    }
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    System.out.println("Close called, bytes written: " + totalWritten
            + " to file: " + mUrl.getPath());
    LOG.trace("Close the output stream for {}", mUrl.toString());
    //response = client.execute(request);
    flush();
    mOutputStream.close();
//    InputStream is = null;
//    try {
//      // Status 400 and up should be read from error stream
//      // Expecting here 201 Create or 202 Accepted
//      int responseCode = response.getStatusLine().getStatusCode();
//      LOG.warn("{}, {}, {}", mUrl.toString(), responseCode,
//              response.getStatusLine().getReasonPhrase());
//      is = response.getEntity().getContent();
//
//      if (responseCode == 401 || responseCode == 403 || responseCode == 407) {
//        // special handling. HTTPconnection already closed stream.
//        LOG.warn("{} : stream closed to {}", responseCode, mUrl.toString());
//      }
//      if (is != null) {
//        is.close();
//      }
//    } catch (Exception e) {
//      if (is != null) {
//        is.close();
//      }
//      LOG.error(e.getMessage());
//      throw e;
//    }
  }

  @Override
  public void flush() throws IOException {
    LOG.trace("{} flush method", mUrl.toString());
  }
}
