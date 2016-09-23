package com.ibm.stocator.fs.common;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import org.apache.hadoop.fs.FileStatus;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.BasicResponseHandler;

import java.io.IOException;
import java.io.InputStream;

public class SwiftObject implements StoredObject {

  JossAccount account;
  String requestURL;
  HttpClient client;
  FileStatus status;

  public SwiftObject(JossAccount acc, String container, String object) {
    account = acc;
    requestURL = acc.getAccessURL() + "/" + container + "/" + object;
    System.out.println(requestURL);
    client = account.getConnectionManager().createHttpConnection();
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
  public boolean createMetadata(String metadata) {
    return false;
  }

  @Override
  public boolean updateMetadata(String metadata) {
    return false;
  }

  @Override
  public boolean deleteMetadata() {
    return false;
  }

  @Override
  public String getMetadata() {
    HttpHead headRequest = new HttpHead(requestURL);
    headRequest.addHeader("X-Auth-Token", account.getAuthToken());
    try {
      HttpResponse response = client.execute(headRequest);
      Header[] headers = response.getAllHeaders();
      for (Header header : headers) {
        System.out.println(header);
        // TODO(@djalova) Create file status from metadata
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public boolean delete() {
    return false;
  }

  @Override
  public FileStatus getFileStatus() {
    FileStatus fs = new FileStatus();

    return null;
  }
}
