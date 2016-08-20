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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.Tuple;
import com.ibm.stocator.fs.swift.auth.JossAccount;

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
   * @return SwiftGETResponse input stream and content length
   * @throws IOException if network issues
   */
  public static SwiftInputStreamWrapper getObject(Path path, JossAccount account)
      throws IOException {
    return getObject(path, account, 0, 0);
  }

  /**
   * GET object
   *
   * @param path path to the object
   * @param account Joss Account wrapper object
   * @param bytesFrom from from
   * @param bytesTo bytes to
   * @return SwiftInputStreamWrapper that includes input stream and length
   * @throws IOException if network errors
   */
  public static SwiftInputStreamWrapper getObject(Path path, JossAccount account,
      long bytesFrom, long bytesTo) throws IOException {
    Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>>  resp = httpGET(path.toString(),
        bytesFrom, bytesTo, account);
    if (resp.x.intValue() >= 400) {
      LOG.warn("Get object {} returned {}", path.toString(), resp.x.intValue());
      LOG.warn("Re-authentication attempt for GET {}", path.toString());
      account.authenticate();
      resp = httpGET(path.toString(), bytesFrom, bytesTo, account);
    }

    SwiftInputStreamWrapper httpStream = new SwiftInputStreamWrapper(
        resp.y.y.getEntity().getContent(), resp.y.x);
    return httpStream;
  }

  /**
   * @param path path to object
   * @param bytesFrom from bytes
   * @param bytesTo to bytes
   * @param account Joss Account object
   * @return Tupple with HTTP response method and HttpRequestBase HttpResponse
   * @throws IOException if error
   */
  private static Tuple<Integer, Tuple<HttpRequestBase, HttpResponse>> httpGET(String path,
      long bytesFrom, long bytesTo, JossAccount account) throws IOException {
    LOG.debug("Prepare HTTP GET {}. From {}, To {}", path, bytesFrom, bytesTo);
    HttpGet httpGet = new HttpGet(path);
    httpGet.addHeader("X-Auth-Token", account.getAuthToken());
    if (bytesTo > 0) {
      final String rangeValue = String.format("bytes=%d-%d", bytesFrom, bytesTo);
      httpGet.addHeader(Constants.RANGES_HTTP_HEADER, rangeValue);
    }
    httpGet.addHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    CloseableHttpClient httpclient = HttpClients.createDefault();
    CloseableHttpResponse response = httpclient.execute(httpGet);
    int responseCode = response.getStatusLine().getStatusCode();
    LOG.debug("GET {} returned with {}", path, responseCode);
    Tuple<HttpRequestBase, HttpResponse> respData = new Tuple<HttpRequestBase,
        HttpResponse>(httpGet, response);
    return new Tuple<Integer, Tuple<HttpRequestBase,
        HttpResponse>>(Integer.valueOf(responseCode), respData);
  }
}
