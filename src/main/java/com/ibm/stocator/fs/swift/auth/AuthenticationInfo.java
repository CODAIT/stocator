package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;
import java.util.Date;

import org.apache.http.HttpResponse;

/**
 * Contains information returned when authenticating an account
 */
abstract class AuthenticationInfo {

  protected String token;
  protected Date tokenExpiration;
  protected String accessUrl;

  public AuthenticationInfo() {
  }

  public AuthenticationInfo(AccountConfiguration config) {

  };

  abstract void parseResponse(HttpResponse response) throws IOException;

  public String getToken() {
    return token;
  }

  public Date getTokenExpiration() {
    return tokenExpiration;
  }

  public String getAccessUrl(){
    return accessUrl;
  }
}
