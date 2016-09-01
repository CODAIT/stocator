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
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

public class SwiftConnectionManager {
  private final PoolingHttpClientConnectionManager connectionPool;

  public SwiftConnectionManager() {
    connectionPool = new PoolingHttpClientConnectionManager();
    connectionPool.setDefaultMaxPerRoute(25);
    connectionPool.setMaxTotal(50);
    SocketConfig socketConfig = SocketConfig.custom()
        .setSoKeepAlive(false).setSoTimeout(10000).build();
    connectionPool.setDefaultSocketConfig(socketConfig);
  }

  private HttpRequestRetryHandler getRetryHandler() {

    HttpRequestRetryHandler myRetryHandler = new HttpRequestRetryHandler() {

      public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
        System.out.println("Execution count " + executionCount);
        if (executionCount >= 100) {
          // Do not retry if over max retry count
          System.out.println(executionCount);
          return false;
        }
        if (exception instanceof NoHttpResponseException) {
          System.out.println("NoHttpResponseException exception");
          return true;
        }
        if (exception instanceof UnknownHostException) {
          // Unknown host
          System.out.println("11");
          return true;
        }
        if (exception instanceof ConnectTimeoutException) {
          // Connection refused
          System.out.println("12");
          return true;
        }
        if (exception instanceof SocketTimeoutException) {
          // Connection refused
          System.out.println("java.net.SocketTimeoutException");
          return true;
        }
        if (exception.getClass() == SocketTimeoutException.class) {
          System.out.println("java.net.SocketTimeoutException - 1");
          return true;
        }
        if (exception.getClass().isInstance(SocketTimeoutException.class)) {
          System.out.println("java.net.SocketTimeoutException - 2");
          return true;
        }
        if (exception instanceof InterruptedIOException) {
          // Timeout
          System.out.println("1");
          return true;
        }
        if (exception instanceof SSLException) {
          // SSL handshake exception
          System.out.println("13");
          return true;
        }
        System.out.println("weird...");
        HttpClientContext clientContext = HttpClientContext.adapt(context);
        HttpRequest request = clientContext.getRequest();
        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
        if (idempotent) {
          // Retry if the request is considered idempotent
          return true;
        }
        System.out.println("return false");
        return false;
      }
    };
    return myRetryHandler;
  }

  public CloseableHttpClient createHttpConnection() {
    /*
     * RequestConfig rConfig = RequestConfig.custom() .setConnectTimeout(5000)
     * .setConnectionRequestTimeout(5000) .setSocketTimeout(5000).build();
     */

    CloseableHttpClient httpclient = HttpClients.custom()
        .setRetryHandler(getRetryHandler())
        .setConnectionManager(connectionPool)
        // .setDefaultRequestConfig(rConfig)
        .build();
    return httpclient;
  }
}
