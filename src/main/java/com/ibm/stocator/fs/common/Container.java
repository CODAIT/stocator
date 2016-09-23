package com.ibm.stocator.fs.common;

import java.io.IOException;
import java.util.Collection;

/**
 * Interface that contains operations that can be performed on containers.
 */
public interface Container {

  /**
   * Creates a container in the object store
   */
  boolean createContainer();

  /**
   * Checks if the container exists
   */
  boolean exists();

  /**
   * Lists objects in container
   */
  Collection<StoredObject> listContainer() throws IOException;

  /**
   * Creates container metadata
   */
  boolean createMetadata(String metadata);

  /**
   * Updates container metadata
   */
  boolean updateMetadata(String metadata);

  /**
   * Deletes container metadata
   */
  boolean deleteMetadata();

  /**
   * Shows container metadata
   */
  String getMetadata();

  /**
   * Deletes container
   */
  boolean delete();

}
