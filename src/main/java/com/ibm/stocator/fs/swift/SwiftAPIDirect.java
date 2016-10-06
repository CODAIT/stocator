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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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

   * Uses the Swift API to retrieve object metadata
   * @param account Account information
   * @param container Container name
   * @param object Object name
   * @param scm Connection Manager
   * @return Returns an HttpResponse containing the metadata as headers
   * @throws IOException
   */
  public static HttpResponse getObjectMetadata(JossAccount account, String container, String object,
                                               SwiftConnectionManager scm) throws IOException {
    HttpHead headRequest = new HttpHead(account.getAccessURL() + "/" + container + "/" + object);
    headRequest.addHeader("X-Auth-Token", account.getAuthToken());
    HttpClient client = scm.createHttpConnection();
    HttpResponse metadata;

    metadata = client.execute(headRequest);
    int responseCode = metadata.getStatusLine().getStatusCode();
    if (responseCode == 404) {
      throw new FileNotFoundException();
    } else if (responseCode < 200 || responseCode >= 300) {
      LOG.error("Get object metadata operation could not be completed.");
      throw new IOException("Get object metadata operation could not be completed. Code: "
              + responseCode);
    }
    return metadata;
  }

  /**
   * Retrieves a list of all objects stored in the container
   * @param account Account info
   * @param manager Connection manager
   * @param container Name of the container
   * @return List of SwiftObjects in the container
   * @throws IOException
   */
  public static Collection<SwiftObject> listContainer(JossAccount account,
                                                      SwiftConnectionManager manager,
                                                      String container) throws IOException {
    return listContainer(account, manager, container, "");
  }

  /**
   * Retrieves list of all objects stored in the container beginning with a prefix
   * @param account Account info
   * @param manager Connection manager
   * @param container Name of the container
   * @param prefix
   * @return List of SwiftObjects in the container
   * @throws IOException
   */
  public static Collection<SwiftObject> listContainer(JossAccount account,
                                                      SwiftConnectionManager manager,
                                                      String container, String prefix)
                                                      throws IOException {

    String requestURL = account.getAccessURL() + "/" + container;
    if (!prefix.isEmpty()) {
      requestURL = requestURL + "?prefix=" + prefix;
    }

    ArrayList<SwiftObject> listing = new ArrayList<>();
    HttpGet getRequest = new HttpGet(requestURL);
    getRequest.addHeader("X-Auth-Token", account.getAuthToken());
    getRequest.addHeader("Accept", "application/json");
    try {
      HttpResponse response = manager.createHttpConnection().execute(getRequest);
      if (response.getStatusLine().getStatusCode() == 200) {
        ResponseHandler handler = new BasicResponseHandler();
        String json = handler.handleResponse(response).toString();
        try {
          JSONArray objectArray = new JSONArray(json);
          for (int i = 0; i < objectArray.length(); i++) {
            JSONObject jsonObject = objectArray.getJSONObject(i);
            DateFormat df = new SimpleDateFormat("YYYY-MM-DD'T'hh:mm:ss");
            long lastModified = df.parse(jsonObject.getString("last_modified")).getTime();
            SwiftObject object = new SwiftObject(jsonObject.getString("name"),
                                                 lastModified,
                                                 jsonObject.getString("content_type"),
                                                 jsonObject.getLong("bytes"));
            listing.add(object);
          }
        } catch (JSONException e) {
          throw new IOException("Response received is not in the expected JSON format.");
        } catch (ParseException e) {
          throw new IOException("Last Modified field is in unexpected format.");
        }
      }

    } catch (IOException e) {
      LOG.error("Unable to complete container listing request.");
      throw new IOException("Unable to complete container listing request.", e);
    }
    return listing;
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
}
