package com.ibm.stocator.fs.swift.auth;

import java.io.UnsupportedEncodingException;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class KeystoneV2AuthenticationRequest extends HttpPost implements AuthenticationRequest {

  private final AccountConfiguration config;

  public KeystoneV2AuthenticationRequest(AccountConfiguration conf) {
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
   * An example request taken from - https://www.swiftstack.com/docs/cookbooks/swift_usage/auth.html
    {
      "auth" : {
        "passwordCredentials" : {
          "username" : "<username>",
          "password" : "<password>"
        },
        "tenantName" : "<tenant>"
      }
    }
   */
  private String createAuthenticationJson() {
    JSONObject object = new JSONObject();
    try {
      JSONObject auth = new JSONObject();
      JSONObject passwordCredentials = new JSONObject();
      passwordCredentials.put("username", config.getUsername());
      passwordCredentials.put("password", config.getPassword());
      auth.put("passwordCredentials", passwordCredentials);
      auth.put("tenantName", config.getTenant());
      object.put("auth", auth);
    } catch (JSONException je) {
      je.printStackTrace();
    }
    return object.toString();
  }
}
