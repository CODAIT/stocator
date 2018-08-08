/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.swift;

import java.io.IOException;
import java.util.HashMap;

import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Utils;

/**
 * Wrapper class adding an internal cache layer for objects metadata,
 * This cache is populated by the list function and on-the-fly requests for objects.
 */
public class SwiftObjectCache {
  private final HashMap<String, SwiftCachedObject> cache;
  private final Container container;
  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftObjectCache.class);

  public SwiftObjectCache(final Container cont) {
    cache = new HashMap<>();
    container = cont;
  }

  /**
   * The get function will first search for the object in the cache.
   * If not found will issue a HEAD request for the object metadata
   * and add the object to the cache.
   *
   * @param objName object name
   * @return cached entry of the object
   * @throws IOException if failed to parse time stamp
   */
  public SwiftCachedObject get(final String objName) throws IOException {
    LOG.trace("Get from cache: {}", objName);
    SwiftCachedObject res = cache.get(objName);
    if (res == null) {
      LOG.trace("Cache get: {} is not in the cache. Access Swift to get content length", objName);
      StoredObject rawObj = container.getObject(removeTrailingSlash(objName));
      if (rawObj != null && rawObj.exists()) {
        res = new SwiftCachedObject(rawObj.getContentLength(),
          Utils.lastModifiedAsLong(rawObj.getLastModified()));
        put(objName, res);
      } else {
        return null;
      }
    }
    return res;
  }

  public void put(String objNameKey, long contentLength, long lastModified) {
    LOG.trace("Add to cache: {} ", objNameKey);
    cache.put(objNameKey, new SwiftCachedObject(contentLength, lastModified));
  }

  private void put(String objName, SwiftCachedObject internalObj) {
    cache.put(objName, internalObj);
  }

  public void remove(String objName) {
    LOG.trace("Remove from cache: {} ", objName);
    cache.remove(objName);
  }

  /**
   * removing the trailing slash because it is not supported in Swift
   * an request on an object (not a container) that has a trailing slash will lead
   * to a 404 response message
   */
  private String removeTrailingSlash(final String objName) {
    if (objName.endsWith("/")) {
      return objName.substring(0, objName.length() - 1);
    }
    return objName;
  }
}
