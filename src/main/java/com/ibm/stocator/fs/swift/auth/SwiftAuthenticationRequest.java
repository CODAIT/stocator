package com.ibm.stocator.fs.swift.auth;

import org.apache.http.client.methods.HttpGet;

public class SwiftAuthenticationRequest extends HttpGet implements AuthenticationRequest {

  public SwiftAuthenticationRequest(AccountConfiguration conf) {
    super(conf.getAuthUrl());

    setHeader("X-Storage-User", conf.getUsername() + ":" + conf.getTenant());
    setHeader("X-Storage-Pass", conf.getPassword());
  }
}
