package com.ibm.stocator.fs.common;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;

public class SwiftObject implements StoredObject {

  JossAccount account;
  String requestURL;
  HttpClient client;
  String containerName;
  String objectName;
  FileStatus status;

  public SwiftObject(JossAccount acc, String container, String object) {
    account = acc;
    containerName = container;
    objectName = object;
    requestURL = acc.getAccessURL() + "/" + container + "/" + object;
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
      String length = response.getFirstHeader("Content-Length").getValue();
      long lastModified = DatatypeConverter.parseDateTime(response.getFirstHeader("Last-Modified")
              .getValue()).getTimeInMillis();

      status = new FileStatus(new Long(length), false, 1,
              128 * 1024 * 1024, lastModified, new Path(""));
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

  @Override
  public String getName() {
    return objectName;
  }
}
