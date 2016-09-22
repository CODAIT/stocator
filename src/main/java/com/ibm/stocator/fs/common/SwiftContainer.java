package com.ibm.stocator.fs.common;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;
import org.apache.http.client.HttpClient;

import java.util.Collection;

public class SwiftContainer implements Container {

  String name;
  HttpClient client;

  public SwiftContainer(String containerName, JossAccount account) {
    name = containerName;
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
  public Collection<StoredObject> listContainer() {
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
    return null;
  }

  @Override
  public boolean delete() {
    return false;
  }
}
