package com.ibm.stocator.fs.common;

import java.io.IOException;
import java.io.InputStream;

import com.ibm.stocator.fs.swift.auth.JossAccount;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;

public class SwiftObject implements StoredObject {

  JossAccount account;
  String requestURL;
  HttpClient client;
  String containerName;
  String objectName;
  int contentLength;
  String lastModified;

  public SwiftObject(JossAccount acc, String container, String object) {
    account = acc;
    containerName = container;
    objectName = object;
    requestURL = acc.getAccessURL() + "/" + container + "/" + object;
    client = account.getConnectionManager().createHttpConnection();
    getMetadata();
  }

  private void getMetadata() {
    HttpHead headRequest = new HttpHead(requestURL);
    headRequest.addHeader("X-Auth-Token", account.getAuthToken());
    try {
      HttpResponse response = client.execute(headRequest);
      contentLength = new Integer(response.getFirstHeader("Content-Length").getValue());
      lastModified = response.getFirstHeader("Last-Modified").getValue();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public boolean createObject() {
    return false;
  }

  @Override
  public InputStream getObject() {
    return null;
  }

  @Override
  public boolean delete() {
    return false;
  }

  @Override
  public String getName() {
    return objectName;
  }

  @Override
  public String getLastModified() {
    return lastModified;
  }

  @Override
  public int getContentLength() {
    return contentLength;
  }

}
