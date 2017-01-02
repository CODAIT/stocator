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
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

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
   * Output stream
   */
  private OutputStream mOutputStream;
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

  private Thread writeThread;
  private long totalWritten;
  private JossAccount mAccount;

  /**
   * Default constructor
   *
   * @param account JOSS account object
   * @param url URL connection
   * @param contentType content type
   * @param metadata input metadata
   * @param connectionManager SwiftConnectionManager
   * @throws IOException if error
   */
  public SwiftOutputStream(JossAccount account, URL url, final String contentType,
                           Map<String, String> metadata, SwiftConnectionManager connectionManager)
          throws IOException {
    mUrl = url;
    totalWritten = 0;
    mAccount = account;
    client = connectionManager.createHttpConnection();
    request = new HttpPut(mUrl.toString());
    request.addHeader("X-Auth-Token", account.getAuthToken());
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
        InputStreamEntity entity = new InputStreamEntity(in, -1);
        entity.setChunked(true);
        entity.setContentType(contentType);
        request.setEntity(entity);
        try {
          LOG.debug("HTTP PUT request {}", mUrl.toString());
          HttpResponse response = client.execute(request);
          int responseCode = response.getStatusLine().getStatusCode();
          LOG.debug("HTTP PUT response {}. Response code {}",
              mUrl.toString(), responseCode);
          if (responseCode == 401) { // Unauthorized error
            mAccount.authenticate();
            request.removeHeaders("X-Auth-Token");
            request.addHeader("X-Auth-Token", mAccount.getAuthToken());
            LOG.warn("Token recreated for {}.  Retry request", mUrl.toString());
            response = client.execute(request);
            responseCode = response.getStatusLine().getStatusCode();
          }
          if (responseCode == 403) { // Unauthorized error
            throw new RuntimeException("403 Access Forbidden");
          }
          if (responseCode >= 400) { // Code may have changed from retrying
            throw new IOException("HTTP Error: " + responseCode
                    + " Reason: " + response.getStatusLine().getReasonPhrase());
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
          interrupt();
        }
      }
    };
    checkThreadState();
  }

  private void checkThreadState() throws IOException {
    if (writeThread.getState().equals(Thread.State.NEW)) {
      Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.error(t.getName() + e);
          if (e.getMessage().startsWith("403")) {
            throw new RuntimeException(e.getMessage(), e);
          }
          t.interrupt();
        }
      };
      writeThread.setUncaughtExceptionHandler(handler);
      writeThread.start();
    } else if (writeThread.isInterrupted()) {
      throw new IOException("Thread was interrupted, write was not completed.");
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + 1;
      LOG.trace("Write {} one byte. Total written {}", mUrl.toString(), totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + len;
      LOG.trace("Write {} off {} len {}. Total {}", mUrl.toString(), off, len, totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + b.length;
      LOG.trace("Write {} len {}. Total {}", mUrl.toString(), b.length, totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    checkThreadState();
    LOG.debug("HTTP PUT close {}", mUrl.toString());
    flush();
    mOutputStream.close();
    try {
      writeThread.join();
    } catch (InterruptedException ie) {
      LOG.error(ie.getMessage());
    }
  }

  @Override
  public void flush() throws IOException {
    LOG.trace("{} flush method", mUrl.toString());
    mOutputStream.flush();
  }
}
