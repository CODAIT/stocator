package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.impl.client.BasicResponseHandler;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Contains information returned when authenticating an account using Keystone V2
 */
public class SwiftV2AuthInfo extends AuthenticationInfo {

  private AccountConfiguration accountConfig;

  public SwiftV2AuthInfo(AccountConfiguration accountConfiguration) throws IOException {
    accountConfig = accountConfiguration;
  }

  @Override
  void parseResponse(HttpResponse response) throws IOException {
    ResponseHandler<String> responseHandler = new BasicResponseHandler();
    String jsonResponse = responseHandler.handleResponse(response);

    try {
      JSONObject obj = new JSONObject(jsonResponse);
      JSONObject accessObj = obj.getJSONObject("access");
      JSONObject tokenObj = accessObj.getJSONObject("token");
      token = tokenObj.getString("id");
      tokenExpiration = DatatypeConverter.parseDateTime(tokenObj.getString("expires")).getTime();

    } catch (JSONException je) {
      je.printStackTrace();
    }
  }
}
