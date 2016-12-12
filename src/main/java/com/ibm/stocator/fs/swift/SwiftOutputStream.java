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

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

import com.ibm.stocator.metrics.DataMetricCounter;
import com.ibm.stocator.metrics.DataMetricUtilities;

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

  private String objName;
  private DataMetricCounter dataMetricCounter;
  private volatile Header clvRequestIdHeader;
  private static final Logger METRICSS_LOG = LoggerFactory.getLogger(DataMetricUtilities.class);

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
   * @param objectName the name of the object to write
   * @throws IOException if error
   */
  public SwiftOutputStream(JossAccount account, URL url, final String contentType,
      Map<String, String> metadata, SwiftConnectionManager connectionManager, String objectName)
          throws IOException {
    long streamStartTime = DataMetricUtilities.getCurrentTimestamp();
    mUrl = url;
    totalWritten = 0;
    mAccount = account;
    objName = objectName;
    client = connectionManager.createHttpConnection();
    request = new HttpPut(mUrl.toString());
    request.addHeader("X-Auth-Token", account.getAuthToken());
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        request.addHeader("X-Object-Meta-" + entry.getKey(), entry.getValue());
      }
    }
    dataMetricCounter = DataMetricUtilities.buildDataMetricCounter(streamStartTime, objName, "PUT");

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
          clvRequestIdHeader = response
              .getFirstHeader(DataMetricUtilities.DSNET_REQUEST_ID_HEADER);
          if (responseCode >= 400) { // Code may have changed from retrying
            throw new IOException("HTTP Error: " + responseCode + " Reason: "
                + response.getStatusLine().getReasonPhrase());
          }
        } catch (IOException e) {
          LOG.error(e.getMessage());
          interrupt();
        }
      }
    };

  }

  private void checkThreadState() throws IOException {
    if (writeThread.getState().equals(Thread.State.NEW)) {
      Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.error(t.getName() + e);
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
    long requestStartTime = DataMetricUtilities.getCurrentTimestamp();

    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + 1;
      LOG.trace("Write {} one byte. Total written {}", mUrl.toString(), totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b);

    DataMetricUtilities.updateDataMetricCounter(dataMetricCounter, 1L, requestStartTime);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    long requestStartTime = DataMetricUtilities.getCurrentTimestamp();

    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + len;
      LOG.trace("Write {} off {} len {}. Total {}", mUrl.toString(), off, len, totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b, off, len);
    DataMetricUtilities.updateDataMetricCounter(dataMetricCounter, len, requestStartTime);
  }

  @Override
  public void write(byte[] b) throws IOException {
    long requestStartTime = DataMetricUtilities.getCurrentTimestamp();

    if (LOG.isTraceEnabled()) {
      totalWritten = totalWritten + b.length;
      LOG.trace("Write {} len {}. Total {}", mUrl.toString(), b.length, totalWritten);
    }
    checkThreadState();
    mOutputStream.write(b);
    DataMetricUtilities.updateDataMetricCounter(dataMetricCounter, b.length, requestStartTime);
  }

  @Override
  public void close() throws IOException {
    long requestStartTime = DataMetricUtilities.getCurrentTimestamp();

    checkThreadState();
    LOG.debug("HTTP PUT close {}", mUrl.toString());
    flush();
    mOutputStream.close();

    try {
      writeThread.join();
    } catch (InterruptedException ie) {
      LOG.error(ie.getMessage());
    }
    DataMetricUtilities.updateDataMetricCounter(dataMetricCounter, 0, requestStartTime);
    if (DataMetricUtilities.isMetricsLoggingEnabled()) {
      String summary = DataMetricUtilities.getCounterSummary(dataMetricCounter, clvRequestIdHeader);
      METRICSS_LOG.trace(summary);
    }
  }

  @Override
  public void flush() throws IOException {
    LOG.trace("{} flush method", mUrl.toString());
    mOutputStream.flush();
  }

}
