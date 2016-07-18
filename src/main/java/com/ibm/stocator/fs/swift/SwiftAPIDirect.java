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

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.Tuple;
import com.ibm.stocator.fs.swift.auth.JossAccount;

/**
 * Direct client to object store implementing Swift API
 * This class is bypass JOSS library
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
  public static SwiftGETResponse getObject(Path path, JossAccount account)
      throws IOException {
    return getObject(path, account, 0, 0);
  }

  /**
   * GET object
   *
   * @param path path to object
   * @param account Joss Account wrapper obejct
   * @param bytesFrom from from
   * @param bytesTo bytes to
   * @return SwiftGETResponse that includes input stream and length
   * @throws IOException if network errors
   */
  public static SwiftGETResponse getObject(Path path, JossAccount account,
      long bytesFrom, long bytesTo) throws IOException {
    Tuple<Integer, GetMethod>  resp = httpGET(path.toString(), bytesFrom, bytesTo, account);
    if (resp.x.intValue() >= 400) {
      LOG.warn("Get object {} returned {}", path.toString(), resp.x.intValue());
      LOG.warn("GET {}. Second try. Re-authentication attempt", path.toString());
      account.authenticate();
      resp = httpGET(path.toString(), bytesFrom, bytesTo, account);
    }

    SwiftInputStreamWrapper httpStream = new SwiftInputStreamWrapper(resp.y);
    SwiftGETResponse getResponse = new SwiftGETResponse(httpStream,
        resp.y.getResponseContentLength());
    return getResponse;
  }

  /**
   * @param path object path
   * @param bytesFrom from bytes
   * @param bytesTo to bytes
   * @param account Joss Account object
   * @return Tupple with HTTP response method and GetMethod
   * @throws HttpException if error
   * @throws IOException if error
   */
  private static Tuple<Integer, GetMethod> httpGET(String path, long bytesFrom,
      long bytesTo, JossAccount account) throws HttpException, IOException {
    GetMethod method = new GetMethod(path);
    method.addRequestHeader(new Header("X-Auth-Token", account.getAuthToken()));
    if (bytesTo > 0) {
      final String rangeValue = String.format("bytes=%d-%d", bytesFrom, bytesTo);
      method.addRequestHeader(new Header(Constants.RANGES_HTTP_HEADER, rangeValue));
    }
    HttpMethodParams methodParams = method.getParams();
    methodParams.setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(3, false));
    methodParams.setIntParameter(HttpConnectionParams.CONNECTION_TIMEOUT, 15000);
    methodParams.setSoTimeout(60000);
    method.addRequestHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    final HttpClient client = new HttpClient();
    int responseCode = client.executeMethod(method);
    return new Tuple<Integer, GetMethod>(Integer.valueOf(responseCode), method);
  }
}
