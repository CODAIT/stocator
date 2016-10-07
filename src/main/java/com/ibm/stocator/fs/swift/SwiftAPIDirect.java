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

import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
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
  public static SwiftInputStreamWrapper getObject(Path path, JossAccount account,
      long bytesFrom, long bytesTo, SwiftConnectionManager scm) throws IOException {
    Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>>  resp = httpGET(path.toString(),
        bytesFrom, bytesTo, account, scm);
    if (resp.x.intValue() >= 400) {
      LOG.warn("Re-authentication attempt for GET {}", path.toString());
      account.authenticate();
      resp = httpGET(path.toString(), bytesFrom, bytesTo, account, scm);
    }

    SwiftInputStreamWrapper httpStream = new SwiftInputStreamWrapper(
        resp.y.y.getEntity(), resp.y.x);
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
  private static Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>> httpGET(String path,
      long bytesFrom, long bytesTo, JossAccount account, SwiftConnectionManager scm)
          throws IOException {
    LOG.debug("HTTP GET {} request. From {}, To {}", path, bytesFrom, bytesTo);
    HttpGet httpGet = new HttpGet(path);
    httpGet.addHeader("X-Auth-Token", account.getAuthToken());
    if (bytesTo > 0) {
      final String rangeValue = String.format("bytes=%d-%d", bytesFrom, bytesTo);
      httpGet.addHeader(Constants.RANGES_HTTP_HEADER, rangeValue);
    }
    httpGet.addHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    CloseableHttpClient httpclient = scm.createHttpConnection();
    CloseableHttpResponse response = httpclient.execute(httpGet);
    int responseCode = response.getStatusLine().getStatusCode();
    LOG.debug("HTTP GET {} response. Status code {}", path, responseCode);
    Tuple<HttpRequestBase, HttpResponse> respData = new Tuple<HttpRequestBase,
        HttpResponse>(httpGet, response);
    return new Tuple<Integer, Tuple<HttpRequestBase,
        HttpResponse>>(Integer.valueOf(responseCode), respData);
  }

  public static HttpResponse deleteObject(JossAccount account, SwiftConnectionManager manager,
                                           String container, String object) throws IOException {

    String requestURL = account.getAccessURL() + "/" + container + "/" + object;
    HttpResponse response;
    HttpDelete deleteRequest = new HttpDelete(requestURL);
    deleteRequest.addHeader("X-Auth-Token", account.getAuthToken());
    try {
      response = manager.createHttpConnection().execute(deleteRequest);
    } catch (IOException e) {
      LOG.error("Delete request could not be completed.");
      throw e;
    }
    return response;
  }

}
