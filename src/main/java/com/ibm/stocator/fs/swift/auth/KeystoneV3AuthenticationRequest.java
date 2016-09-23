package com.ibm.stocator.fs.swift.auth;

import java.io.UnsupportedEncodingException;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class KeystoneV3AuthenticationRequest extends HttpPost implements AuthenticationRequest {

  private final AccountConfiguration config;

  public KeystoneV3AuthenticationRequest(AccountConfiguration conf) {
    super(conf.getAuthUrl());
    config = conf;
    String messageBody = "";

    messageBody = createAuthenticationJson();

    try {
      StringEntity entity = new StringEntity(messageBody);
      setEntity(entity);
      setHeader("Content-type", "application/json");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates a JSON format String to request authentication
   * An example request taken from - http://developer.openstack.org/api-ref/identity/v3/
    {
        "auth": {
          "identity": {
            "methods": [
              "password"
            ],
            "password": {
              "user": {
                "id": "ee4dfb6e5540447cb3741905149d9b6e",
                "password": "devstacker"
              }
            }
          },
        "scope": {
          "project": {
            "id": "a6944d763bf64ee6a275f1263fae0352"
          }
        }
      }
    }
   */
  private String createAuthenticationJson() {
    JSONObject object = new JSONObject();
    try {
      JSONObject auth = new JSONObject();
      JSONObject identity = new JSONObject();
      JSONArray method = new JSONArray();
      JSONObject password = new JSONObject();
      JSONObject user = new JSONObject();
      JSONObject scope = new JSONObject();
      JSONObject project = new JSONObject();

      project.put("id", config.getProjectId());
      scope.put("project", project);
      user.put("id", config.getUsername());
      user.put("password", config.getPassword());
      password.put("user", user);
      method.put("password");
      identity.put("methods", method);
      identity.put("password", password);
      auth.put("identity", identity);
      auth.put("scope", scope);
      object.put("auth", auth);
    } catch (JSONException je) {
      je.printStackTrace();
    }
    return object.toString();
  }
}
