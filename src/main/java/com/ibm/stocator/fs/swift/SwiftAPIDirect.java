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

import com.ibm.stocator.fs.common.Constants;

import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;

/**
 * Direct client to object store implementing Swift API
 * This class is bypass JOSS library
 */
public class SwiftAPIDirect {

  /**
   *
   * @param path path to object
   * @param authToken authentication token
   * @return HttpResponse input stream and content length
   * @throws IOException if network issues
   */
  public static HttpResponse getObject(Path path, HttpClient httpClient, String authToken)
      throws IOException {
    return getObject(path, httpClient, authToken, 0, 0);
  }

  /**
   * GET object
   *
   * @param path path to object
   * @param authToken authentication token
   * @param bytesFrom from from
   * @param bytesTo bytes to
   * @return HttpResponse that includes input stream and length
   * @throws IOException if network errors
   */
  public static HttpResponse getObject(final Path path, HttpClient httpClient, String authToken,
                                           long bytesFrom, long bytesTo) throws IOException {

    HttpGet request = new HttpGet(path.toUri());
    request.addHeader(new BasicHeader("X-Auth-Token", authToken));

    if (bytesTo > 0) {
      final String rangeValue = String.format("bytes=%d-%d", bytesFrom, bytesTo);
      request.addHeader(new BasicHeader(Constants.RANGES_HTTP_HEADER, rangeValue));
    }

    request.addHeader(Constants.USER_AGENT_HTTP_HEADER, Constants.STOCATOR_USER_AGENT);
    HttpHost host = new HttpHost(path.toUri().getHost());
    HttpResponse response = null;

    try {
      response = httpClient.execute(host, request);
      return response;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return response;
  }
}
