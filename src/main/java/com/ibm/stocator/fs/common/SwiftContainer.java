package com.ibm.stocator.fs.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;

import com.ibm.stocator.fs.swift.auth.JossAccount;

public class SwiftContainer implements Container {

  String name;
  HttpClient client;
  JossAccount account;
  String requestURL;

  public SwiftContainer(String containerName, JossAccount acc) {
    name = containerName;
    account = acc;
    requestURL = account.getAccessURL() + "/" + containerName;
    client = account.getConnectionManager().createHttpConnection();
  }

  @Override
  public boolean createContainer() {
    return false;
  }

  @Override
  public boolean exists() {
    return false;
  }

  @Override
  public Collection<StoredObject> listContainer() throws IOException {
    return listContainer("");
  }

  @Override
  public Collection<StoredObject> listContainer(String prefix) throws IOException {
    Collection<StoredObject> list = new ArrayList<>();
    HttpGet getRequest = new HttpGet(requestURL + "?prefix=" + prefix);
    System.out.println("URL: " + getRequest.toString());
    getRequest.addHeader("X-Auth-Token", account.getAuthToken());
    HttpResponse response = client.execute(getRequest);
    if (response.getStatusLine().getStatusCode() == 200) {
      ResponseHandler handler = new BasicResponseHandler();
      String[] objectNames = handler.handleResponse(response).toString().split("\n");
      for (String objName : objectNames) {
        SwiftObject obj = new SwiftObject(account, name, objName);
        list.add(obj);
      }
    }
    return list;
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
    return null;
  }

  @Override
  public boolean delete() {
    return false;
  }
}
