package com.ibm.stocator.fs.swift;

public class SwiftObject {

  private String objectName;
  private long contentLength;
  private String contentType;
  private long lastModified;

  public SwiftObject(String name,long modified, String content, long length) {
    objectName = name;
    lastModified = modified;
    contentType = content;
    contentLength = length;
  }

  public String getObjectName() {
    return objectName;
  }

  public long getContentLength() {
    return contentLength;
  }

  public String getContentType() {
    return contentType;
  }

  public long getLastModified() {
    return lastModified;
  }
}
