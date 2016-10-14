package com.ibm.stocator.fs.swift.auth;

import org.apache.http.client.methods.HttpGet;

public class SwiftAuthenticationRequest extends HttpGet implements AuthenticationRequest {

  public SwiftAuthenticationRequest(AccountConfiguration conf) {
    super(conf.getAuthUrl());

    String user =  conf.getUsername();
    if (conf.getTenant() != null) {
      user += conf.getTenant();
    }
    setHeader("X-Storage-User", user);
    setHeader("X-Storage-Pass", conf.getPassword());

  }
}
