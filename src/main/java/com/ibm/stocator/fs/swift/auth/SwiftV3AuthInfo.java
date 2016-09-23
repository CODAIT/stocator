package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.impl.client.BasicResponseHandler;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Contains information returned when authenticating an account using Keystone V3
 */
public class SwiftV3AuthInfo extends AuthenticationInfo {

  public SwiftV3AuthInfo() {

  }

  public SwiftV3AuthInfo(HttpResponse response) throws IOException {
    parseResponse(response);
  }

  @Override
  void parseResponse(HttpResponse response) throws IOException {

    ResponseHandler<String> responseHandler = new BasicResponseHandler();
    String jsonResponse = responseHandler.handleResponse(response);
    token = response.getFirstHeader("X-Subject-Token").getValue();

    try {
      JSONObject obj = new JSONObject(jsonResponse);
      JSONObject tokenObj = obj.getJSONObject("token");
      tokenExpiration = DatatypeConverter.parseDateTime(tokenObj.getString("expires_at")).getTime();

    } catch (JSONException je) {
      je.printStackTrace();
    }
  }
}
