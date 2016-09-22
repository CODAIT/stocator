package com.ibm.stocator.fs.common;

import org.apache.hadoop.fs.FileStatus;

import java.io.InputStream;

/**
 * Interface that contains operations that can be performed on objects.
 */
interface StoredObject {

  /**
   * Creates a container in the object store
   */
  boolean createObject();

  /**
   * Get object content
   */
  InputStream getObject();

  /**
   * Creates object metadata
   */
  boolean createMetadata(String metadata);

  /**
   * Updates object metadata
   */
  boolean updateMetadata(String metadata);

  /**
   * Deletes object metadata
   */
  boolean deleteMetadata();

  /**
   * Shows object metadata
   */
  String getMetadata();

  /**
   * Deletes object
   */
  boolean delete();

  /**
   * Gets FileStatus of object
   */
  FileStatus getFileStatus();

}
