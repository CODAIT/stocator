package com.ibm.stocator.fs.common;

import org.apache.hadoop.fs.FileStatus;

import java.io.InputStream;

public class SwiftObject implements StoredObject {

  FileStatus status;

  public SwiftObject() {

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
    return null;
  }

  @Override
  public boolean delete() {
    return false;
  }

  @Override
  public FileStatus getFileStatus() {
    return null;
  }
}
