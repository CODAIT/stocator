package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;
import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.impl.client.BasicResponseHandler;
import org.codehaus.jettison.json.JSONArray;
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

      JSONArray serviceCatalog = accessObj.getJSONArray("serviceCatalog");

      JSONArray swiftEndpoints = null;
      for (int i = 0; i < serviceCatalog.length() && swiftEndpoints == null; i++) {
        JSONObject service = serviceCatalog.getJSONObject(i);
        if (service.getString("name").equals("swift")) {
          swiftEndpoints = service.getJSONArray("endpoints");
        }
      }

      if (swiftEndpoints == null) {
        throw new IOException("No swift endpoints exist");
      }

      String isPublic = accountConfig.getPublic() ? "public" : "internal";
      for (int i = 0; i < swiftEndpoints.length(); i++) {
        JSONObject endpoint = swiftEndpoints.getJSONObject(i);
        if (endpoint.get("interface").equals(isPublic)) {
          if (accountConfig.getRegion() != null) {
            // Return URL that matches region and interface
            if (accountConfig.getRegion().equals(endpoint.getString("region"))) {
              accessUrl = endpoint.getString("url");
            }
          } else {
            // No region preference, return any URL
            accessUrl = endpoint.getString("url");
          }
        }
      }

    } catch (JSONException je) {
      je.printStackTrace();
    }

    if (accessUrl == null) {
      throw new IOException("Unable to get url with provided public and a region configs");
    }
  }
}
