package com.ibm.stocator.fs.common;

import java.io.InputStream;

/**
 * Interface that contains operations that can be performed on objects.
 */
public interface StoredObject {

  /**
   * Creates a container in the object store
   */
  boolean createObject();

  /**
   * Get object content
   */
  InputStream getObject();

  /**
   * Deletes object
   */
  boolean delete();

  /**
   * Gets object name
   */
  String getName();

  /**
   * Returns last modified
   */
  String getLastModified();

  int getContentLength();

}
