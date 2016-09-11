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

package com.ibm.stocator.fs.swift.http;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import javax.net.ssl.SSLException;

import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection manager for Swift API object store
 * Use pooling connection pool manager and custom retry handler
 * The same pooling manager is used by other components and not only by JOSS
 */
public class SwiftConnectionManager {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftConnectionManager.class);
  /*
   * Connection pool
   */
  private final PoolingHttpClientConnectionManager connectionPool;

  /**
   * Default constructor
   */
  public SwiftConnectionManager() {
    connectionPool = new PoolingHttpClientConnectionManager();
    connectionPool.setDefaultMaxPerRoute(25);
    connectionPool.setMaxTotal(50);
    SocketConfig socketConfig = SocketConfig.custom()
        .setSoKeepAlive(false).setSoTimeout(10000).build();
    connectionPool.setDefaultSocketConfig(socketConfig);
  }

  /**
   * Creates custom retry handler to be used if HTTP exception happens
   *
   * @return retry handler
   */
  private HttpRequestRetryHandler getRetryHandler() {

    HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {

      public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        System.out.println(executionCount);
        if (executionCount >= 100) {
          // Do not retry if over max retry count
          LOG.debug("Execution count {} is bigger then threashold. Stop" ,executionCount);
          return false;
        }
        if (exception instanceof NoHttpResponseException) {
          LOG.debug("NoHttpResponseException exception. Retry count {}", executionCount);
          return true;
        }
        if (exception instanceof UnknownHostException) {
          LOG.debug("UnknownHostException. Retry count {}", executionCount);
          return true;
        }
        if (exception instanceof ConnectTimeoutException) {
          LOG.debug("ConnectTimeoutException. Retry count {}", executionCount);
          return true;
        }
        if (exception instanceof SocketTimeoutException
            || exception.getClass() == SocketTimeoutException.class
            || exception.getClass().isInstance(SocketTimeoutException.class)) {
          // Connection refused
          LOG.debug("socketTimeoutException Retry count {}", executionCount);
          return true;
        }
        if (exception instanceof InterruptedIOException) {
          // Timeout
          LOG.debug("InterruptedIOException Retry count {}", executionCount);
          return true;
        }
        if (exception instanceof SSLException) {
          LOG.debug("SSLException Retry count {}", executionCount);
          return true;
        }
        HttpClientContext clientContext = HttpClientContext.adapt(context);
        HttpRequest request = clientContext.getRequest();
        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
        if (idempotent) {
          LOG.debug("HttpEntityEnclosingRequest. Retry count {}", executionCount);
          return true;
        }
        LOG.debug("Retry stopped. Retry count {}", executionCount);
        return false;
      }
    };
    return myRetryHandler;
  }

  /**
   * Creates HTTP connection based on the connection pool
   *
   * @return http client
   */
  public CloseableHttpClient createHttpConnection() {
    RequestConfig rConfig = RequestConfig.custom().setExpectContinueEnabled(true)
        .setConnectTimeout(5000)
        .setConnectionRequestTimeout(5000).setSocketTimeout(5000)
        .build();

    CloseableHttpClient httpclient = HttpClients.custom()
        .setRetryHandler(getRetryHandler())
        .setConnectionManager(connectionPool)
        .setDefaultRequestConfig(rConfig)
        .build();
    return httpclient;
  }
}
