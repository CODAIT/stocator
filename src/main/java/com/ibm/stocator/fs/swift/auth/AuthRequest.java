package com.ibm.stocator.fs.swift.auth;

import java.io.UnsupportedEncodingException;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class AuthRequest extends HttpPost {
  private AccountConfiguration config;

  public AuthRequest(AccountConfiguration conf) {
    super(conf.getAuthUrl());
    config = conf;
    String authMethod = config.getAuthMethod();
    String messageBody = "";

    messageBody = createKeystoneV3Json();

    // TODO(djalova) authenticate for keystone v1

    // TODO(djalova) authenticate for tempauth

    try {
      StringEntity entity = new StringEntity(messageBody);
      setEntity(entity);
      setHeader("Content-type", "application/json");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }

  private String createKeystoneV3Json() {
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
