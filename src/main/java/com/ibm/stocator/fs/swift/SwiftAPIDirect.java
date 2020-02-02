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
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.Tuple;
import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

/**
 * Direct client to the object store implementing Swift API
 * This class bypass JOSS library and uses JOSS only to obtain access token and authenticated url
 */
public class SwiftAPIDirect {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftAPIDirect.class);

  /**
   * Get object
   *
   * @param path path to object
   * @param account Joss account wrapper object
   * @param scm Swift connection manager
   * @return SwiftGETResponse input stream and content length
   * @throws IOException if network issues
   */
  public static SwiftInputStreamWrapper getObject(Path path, JossAccount account,
      SwiftConnectionManager scm)
      throws IOException {
    return getObject(path, account, 0, 0, scm);
  }

  /**
   * GET object
   *
   * @param path path to the object
   * @param account Joss Account wrapper object
   * @param bytesFrom from from
   * @param bytesTo bytes to
   * @param scm Swift Connection manager
   * @return SwiftInputStreamWrapper that includes input stream and length
   * @throws IOException if network errors
   */
  public static SwiftInputStreamWrapper getObject(
          final Path path,
          final JossAccount account,
          final long bytesFrom,
          final long bytesTo,
          final SwiftConnectionManager scm) throws IOException {
    Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>> resp = httpGET(
            path.toString(),
            bytesFrom,
            bytesTo,
            account,
            scm);
    if (resp.x.intValue() >= 400) {
      LOG.warn("Re-authentication attempt for GET {}", path.toString());
      account.authenticate();
      resp = httpGET(path.toString(), bytesFrom, bytesTo, account, scm);
    }

    final SwiftInputStreamWrapper httpStream = new SwiftInputStreamWrapper(
        resp.y.y.getEntity(), resp.y.x
    );
    return httpStream;
  }

  /**
   * @param path path to object
   * @param bytesFrom from bytes
   * @param bytesTo to bytes
   * @param account JOSS Account object
   * @return Tuple with HTTP response method and HttpRequestBase HttpResponse
   * @throws IOException if error
   */
  private static Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>> httpGET(
          final String path,
          final long bytesFrom,
          final long bytesTo,
          final JossAccount account,
          final SwiftConnectionManager scm) throws IOException {
    LOG.debug("HTTP GET {} request. From {}, To {}", path, bytesFrom, bytesTo);
    final HttpGet httpGet = new HttpGet(path);
    httpGet.addHeader("X-Auth-Token", account.getAuthToken());
    if (bytesTo > 0) {
      final String rangeValue = String.format("bytes=%d-%d", bytesFrom, bytesTo);
      httpGet.addHeader(Constants.RANGES_HTTP_HEADER, rangeValue);
    }
    httpGet.addHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    final CloseableHttpClient httpclient = scm.createHttpConnection();
    final CloseableHttpResponse response = httpclient.execute(httpGet);
    int responseCode = response.getStatusLine().getStatusCode();
    LOG.debug("HTTP GET {} response. Status code {}", path, responseCode);
    final Tuple<HttpRequestBase, HttpResponse> respData = new Tuple<HttpRequestBase,
        HttpResponse>(httpGet, response);
    return new Tuple<Integer, Tuple<HttpRequestBase,
        HttpResponse>>(Integer.valueOf(responseCode), respData);
  }

  /**
   * @param path path to object
   * @param inputStream input stream
   * @param account JOSS Account object
   * @param metadata custom metadata
   * @param size the object size
   * @param type the content type
   * @return HTTP response code
   * @throws IOException if error
   */
  private static int httpPUT(String path,
      InputStream inputStream, JossAccount account, SwiftConnectionManager scm,
      Map<String, String> metadata, long size, String type)
          throws IOException {
    LOG.debug("HTTP PUT {} request on {}", path);
    final HttpPut httpPut = new HttpPut(path);
    httpPut.addHeader("X-Auth-Token", account.getAuthToken());
    httpPut.addHeader("Content-Type", type);
    httpPut.addHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    if (metadata != null && !metadata.isEmpty()) {
      for (Map.Entry<String, String> entry : metadata.entrySet()) {
        httpPut.addHeader("X-Object-Meta-" + entry.getKey(), entry.getValue());
      }
    }
    final RequestConfig config = RequestConfig.custom().setExpectContinueEnabled(true)
        .setNormalizeUri(false).build();
    httpPut.setConfig(config);
    final InputStreamEntity entity = new InputStreamEntity(inputStream, size);
    httpPut.setEntity(entity);
    final CloseableHttpClient httpclient = scm.createHttpConnection();
    final CloseableHttpResponse response = httpclient.execute(httpPut);
    int responseCode = response.getStatusLine().getStatusCode();
    LOG.debug("HTTP PUT {} response. Status code {}", path, responseCode);
    response.close();
    return responseCode;
  }

  /**
   * PUT object
   *
   * @param path path to the object
   * @param account Joss Account wrapper object
   * @param inputStream InputStream
   * @param scm Swift Connection manager
   * @param metadata custom metadata
   * @param size the object size
   * @param type the content type
   * @return HTTP Response code
   * @throws IOException if network errors
   */
  public static int putObject(
          final String path,
          final JossAccount account,
          final InputStream inputStream,
          final SwiftConnectionManager scm,
          final Map<String, String> metadata,
          final long size,
          final String type) throws IOException {
    int resp = httpPUT(path, inputStream, account, scm, metadata, size, type);
    if (resp >= 400) {
      LOG.warn("Re-authentication attempt for GET {}", path);
      account.authenticate();
      resp = httpPUT(path.toString(),inputStream, account, scm, metadata, size, type);
    }
    return resp;
  }

  /*
   * Sends a HEAD request to get an object's length
   */
  public static long getTempUrlObjectLength(Path path, SwiftConnectionManager scm)
          throws IOException {
    final HttpHead head = new HttpHead(path.toString().replace("swift2d", "https"));
    final CloseableHttpResponse response = scm.createHttpConnection().execute(head);
    return Long.parseLong(response.getFirstHeader("Content-Length").getValue());
  }

}
