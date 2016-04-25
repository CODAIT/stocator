package com.ibm.stocator.fs.swift;

public class SwiftGETResponse {
  /*
   * Response size
   */
  private long responseSize;
  /*
   * Swift input stream wrapper
   */
  private SwiftInputStreamWrapper streamWrapper;

  /**
   * Constructor
   *
   * @param inputStream Swift input stream
   * @param size data size
   */
  public SwiftGETResponse(SwiftInputStreamWrapper inputStream, long size) {
    responseSize = size;
    streamWrapper = inputStream;
  }

  /**
   * Get response size
   *
   * @return response size
   */
  public long getResponseSize() {
    return responseSize;
  }

  /**
   * Get Swift input stream wrapper
   *
   * @return Swift input stream wrapper
   */
  public SwiftInputStreamWrapper getStreamWrapper() {
    return streamWrapper;
  }

}
