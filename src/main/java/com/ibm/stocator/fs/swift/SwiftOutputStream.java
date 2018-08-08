/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  (C) Copyright IBM Corp. 2015, 2016
 */

package com.ibm.stocator.fs.swift;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * Wraps OutputStream
 * This class is not thread-safe
 */
public class SwiftOutputStream extends OutputStream {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftOutputStream.class);

  /*
   *  Executor service to handle the HTTP PUT connection
   */
  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  /*
   * Access url
   */
  private final URL mUrl;

  /*
   * Client used
   */
  private final CloseableHttpClient client;

  // Holds the pending http request
  private Future<Integer> futureTask;

  // Keep track of the number of bytes written
  private long totalWritten = 0L;

  private JossAccount mAccount;
  private HttpPut request;

  private final String contentType;
  private final AtomicBoolean openConnection = new AtomicBoolean(false);

  private final PipedOutputStream pipOutStream;
  private final BufferedOutputStream bufOutStream;

  /**
   * Default constructor
   *
   * @param account           JOSS account object
   * @param url               URL connection
   * @param targetContentType Content type
   * @param metadata          input metadata
   * @param connectionManager SwiftConnectionManager
   */
  public SwiftOutputStream(
      JossAccount account,
      URL url,
      final String targetContentType,
      Map<String, String> metadata,
      SwiftConnectionManager connectionManager
  ) {
    mUrl = url;
    mAccount = account;
    client = connectionManager.createHttpConnection();
    contentType = targetContentType;

    pipOutStream = new PipedOutputStream();
    bufOutStream = new BufferedOutputStream(pipOutStream, Constants.SWIFT_DATA_BUFFER);

    // Append the headers to the request
    request = new HttpPut(mUrl.toString());
    request.addHeader("X-Auth-Token", account.getAuthToken());
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        request.addHeader("X-Object-Meta-" + entry.getKey(), entry.getValue());
      }
    }
  }

  private synchronized void OpenHttpConnection() throws IOException {
    // Let know that the connection is open
    if (!openConnection.get()) {
      final PipedInputStream in = new PipedInputStream();
      pipOutStream.connect(in);

      Callable<Integer> connectionTask = new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          LOG.info("Invoke HTTP Put request");

          int responseCode;
          String reasonPhrase;

          InputStreamEntity entity = new InputStreamEntity(in, -1);
          entity.setChunked(true);
          entity.setContentType(contentType);
          request.setEntity(entity);

          LOG.info("HTTP PUT request {}", mUrl.toString());

          openConnection.set(true);
          try (CloseableHttpResponse response = client.execute(request)) {
            responseCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
          }
          LOG.info("HTTP PUT response {}. Response code {}", mUrl.toString(), responseCode);
          if (responseCode == HTTP_UNAUTHORIZED) { // Unauthorized error
            mAccount.authenticate();
            request.removeHeaders("X-Auth-Token");
            request.addHeader("X-Auth-Token", mAccount.getAuthToken());
            LOG.warn("Token recreated for {}.  Retry request", mUrl.toString());
            try (CloseableHttpResponse response = client.execute(request)) {
              responseCode = response.getStatusLine().getStatusCode();
              reasonPhrase = response.getStatusLine().getReasonPhrase();
            }
          }
          if (responseCode >= HTTP_BAD_REQUEST) { // Code may have changed from retrying
            throw new IOException("HTTP Error: " + responseCode + " Reason: " + reasonPhrase);
          }
          return responseCode;
        }
      };
      futureTask = executor.submit(connectionTask);
      do {
        // Wait till the connection is open and the task isn't done
      } while (!openConnection.get() && !futureTask.isDone());
    }
  }

  private void checkBuffer() throws IOException {
    checkBuffer(1);
  }

  private void checkBuffer(int toBeWritten) throws IOException {
    totalWritten += toBeWritten;

    if (LOG.isTraceEnabled()) {
      LOG.trace("{} written ({} buffer size)", totalWritten, Constants.SWIFT_DATA_BUFFER);
    }

    if (totalWritten >= Constants.SWIFT_DATA_BUFFER) {
      OpenHttpConnection();
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Check if we need to open the connection
    checkBuffer(len);

    bufOutStream.write(b, off, len);
  }

  @Override
  public void write(int b) throws IOException {

    // Check if we need to open the connection
    checkBuffer();

    bufOutStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void close() throws IOException {
    LOG.info("HTTP PUT close {}", mUrl.toString());

    // Make sure that we have a HTTP connection
    OpenHttpConnection();

    flush();

    bufOutStream.close();
    pipOutStream.close();

    try {
      int responseCode = futureTask.get();
      futureTask.cancel(true);
      executor.shutdown();
      if (responseCode >= HTTP_BAD_REQUEST) { // Code may have changed from retrying
        throw new IOException("Could not PUT contents, got HTTP " + responseCode);
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    LOG.info("{} flush method", mUrl.toString());
    bufOutStream.flush();
    pipOutStream.flush();
  }
}
