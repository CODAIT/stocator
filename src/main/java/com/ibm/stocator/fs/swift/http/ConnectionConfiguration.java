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

package com.ibm.stocator.fs.swift.http;

/**
 * HTTP Connection configuration
 *
 */
public class ConnectionConfiguration {

  public static final int DEFAULT_MAX_PER_ROUTE = 25;
  public static final int DEFAULT_MAX_TOTAL_CONNECTIONS = 50;
  public static final int DEFAULT_SOCKET_TIMEOUT = 10000;
  public static final int DEFAULT_EXECUTION_RETRY = 100;
  public static final int DEFAULT_REQUEST_CONNECT_TIMEOUT = 5000;
  public static final int DEFAULT_REQUEST_CONNECTION_TIMEOUT = 5000;
  public static final int DEFAULT_REQUEST_SOCKET_TIMEOUT = 50000;
  public static final String TLS_V12 = "TLSv1.2";
  public static final String DEFAULT_TLS = "default";

  /*
   * maximal connections per IP route
   */
  private int maxPerRoute;
  /*
   * maximal concurrent connections
   */
  private int maxTotal;
  /*
   * low level socket timeout in milliseconds
   */
  private int soTimeout;
  /*
   * number of retries for certain HTTP issues
   */
  private int executionCount;
  /*
   * Request level connect timeout
   * Determines the timeout in milliseconds until a connection is established
   */
  private int reqConnectTimeout;
  /*
   * Request level connection timeout
   * Returns the timeout in milliseconds used when requesting a connection from the
   * connection manager
   */
  private int reqConnectionRequestTimeout;
  /*
   * Defines the socket timeout (SO_TIMEOUT) in milliseconds,
   * which is the timeout for waiting for data or, put differently,
   * a maximum period inactivity between two consecutive data packets).
   */
  private int reqSocketTimeout;
  private String newTLSVersion = null;

  public ConnectionConfiguration() {
  }

  public void setTLSVersion(String newTLS) {
    newTLSVersion = newTLS;
  }

  public String getNewTLSVersion() {
    return newTLSVersion;
  }

  public boolean isNewTLSneed() {
    if (newTLSVersion != null && !newTLSVersion.equals(DEFAULT_TLS)) {
      return true;
    }
    return false;
  }

  public int getMaxPerRoute() {
    return maxPerRoute;
  }

  public void setMaxPerRoute(int maxPerRouteT) {
    maxPerRoute = maxPerRouteT;
  }

  public int getMaxTotal() {
    return maxTotal;
  }

  public void setMaxTotal(int maxTotalT) {
    maxTotal = maxTotalT;
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public void setSoTimeout(int soTimeoutT) {
    soTimeout = soTimeoutT;
  }

  public int getExecutionCount() {
    return executionCount;
  }

  public void setExecutionCount(int executionCountT) {
    executionCount = executionCountT;
  }

  public int getReqConnectTimeout() {
    return reqConnectTimeout;
  }

  public void setReqConnectTimeout(int reqConnectTimeoutT) {
    reqConnectTimeout = reqConnectTimeoutT;
  }

  public int getReqConnectionRequestTimeout() {
    return reqConnectionRequestTimeout;
  }

  public void setReqConnectionRequestTimeout(int reqConnectionRequestTimeoutT) {
    reqConnectionRequestTimeout = reqConnectionRequestTimeoutT;
  }

  public int getReqSocketTimeout() {
    return reqSocketTimeout;
  }

  public void setReqSocketTimeout(int reqSocketTimeoutT) {
    reqSocketTimeout = reqSocketTimeoutT;
  }

  @Override
  public String toString() {
    return "ConnectionConfiguration [maxPerRoute=" + maxPerRoute + ", maxTotal=" + maxTotal
        + ", soTimeout=" + soTimeout
        + ", executionCount=" + executionCount + ", reqConnectTimeout="
        + reqConnectTimeout
        + ", reqConnectionRequestTimeout=" + reqConnectionRequestTimeout
        + ", reqSocketTimeout=" + reqSocketTimeout
        + "]";
  }

}
