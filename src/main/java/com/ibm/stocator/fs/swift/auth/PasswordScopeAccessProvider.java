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

package com.ibm.stocator.fs.swift.auth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.javaswift.joss.client.factory.AuthenticationMethod.AccessProvider;
import org.javaswift.joss.model.Access;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeyStone V3 authentication
 * Covers Password Scoped Authentication
 * Implements abstract AccessProvider
 *
 */
public class PasswordScopeAccessProvider implements AccessProvider {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(PasswordScopeAccessProvider.class);

  /*
   * User ID
   */
  private String mUserId;

  /*
   * Password ID
   */
  private String mPassword;

  /*
   * Project ID
   */
  private String mProjectId;

  /*
   * Authentication URL
   */
  private String mAuthUrl;

  /*
   * Preferred Region
   */
  private String mPrefferedRegion;

  /**
   * Support for Keystone V3 API
   * Password Scoped Authentication
   *
   * @param userId user id
   * @param password password
   * @param projectId project id
   * @param authUrl authentication url
   * @param prefferedRegion Keystone preffered region
   */
  public PasswordScopeAccessProvider(String userId, String password,
      String projectId, String authUrl, String prefferedRegion) {
    mUserId = userId;
    mPassword = password;
    mProjectId = projectId;
    mAuthUrl = authUrl;
    mPrefferedRegion = prefferedRegion;
  }

  /**
   * Authentication logic
   *
   * @return Access JOSS access object
   * @throws IOException if failed to parse the response
   */
  public Access passwordScopeAuth() throws IOException {
    InputStreamReader reader = null;
    BufferedReader bufReader = null;
    try {
      JSONObject user = new JSONObject();
      user.put("id", mUserId);
      user.put("password", mPassword);
      JSONObject password = new JSONObject();
      password.put("user", user);
      JSONArray methods = new JSONArray();
      methods.add("password");
      JSONObject identity = new JSONObject();
      identity.put("methods", methods);
      identity.put("password", password);
      JSONObject project = new JSONObject();
      project.put("id", mProjectId);
      JSONObject scope = new JSONObject();
      scope.put("project", project);
      JSONObject auth = new JSONObject();
      auth.put("identity", identity);
      auth.put("scope", scope);
      JSONObject requestBody = new JSONObject();
      requestBody.put("auth", auth);
      HttpURLConnection connection =
          (HttpURLConnection) new URL(mAuthUrl).openConnection();
      connection.setDoOutput(true);
      connection.setRequestProperty("Accept", "application/json");
      connection.setRequestProperty("Content-Type", "application/json");
      OutputStream output = connection.getOutputStream();
      output.write(requestBody.toString().getBytes());
      int status = connection.getResponseCode();
      if (status != 201) {
        return null;
      }
      reader = new InputStreamReader(connection.getInputStream());
      bufReader = new BufferedReader(reader);
      String res = bufReader.readLine();
      JSONParser parser = new JSONParser();
      JSONObject jsonResponse = (JSONObject) parser.parse(res);

      String token = connection.getHeaderField("X-Subject-Token");
      PasswordScopeAccess access = new PasswordScopeAccess(jsonResponse, token,
          mPrefferedRegion);
      bufReader.close();
      reader.close();
      connection.disconnect();
      return access;

    } catch (Exception e) {
      if (bufReader != null) {
        bufReader.close();
      }
      if (reader != null) {
        reader.close();
      }
      throw new IOException(e);
    }
  }

  @Override
  public Access authenticate() {
    try {
      return passwordScopeAuth();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return null;
    }
  }
}
