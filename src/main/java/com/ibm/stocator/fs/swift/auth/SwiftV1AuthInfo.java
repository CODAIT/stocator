package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;

import org.apache.http.HttpResponse;

/**
 * Contains information returned when authenticating an account using Keystone V3
 */
public class SwiftV1AuthInfo extends AuthenticationInfo {

  public SwiftV1AuthInfo() {

  }

  public SwiftV1AuthInfo(HttpResponse response) throws IOException {
    parseResponse(response);
  }

  @Override
  void parseResponse(HttpResponse response) throws IOException {
    token = response.getFirstHeader("X-Auth-Token").getValue();
  }

}
