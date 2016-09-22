package com.ibm.stocator.fs.swift.auth;

import org.apache.http.client.methods.HttpGet;

public class SwiftAuthenticationRequest extends HttpGet implements AuthenticationRequest {

  public SwiftAuthenticationRequest(AccountConfiguration conf) {
    super(conf.getAuthUrl());

    setHeader("X-Auth-User", conf.getUsername());
    setHeader("X-Auth-Key", conf.getPassword());
  }
}
