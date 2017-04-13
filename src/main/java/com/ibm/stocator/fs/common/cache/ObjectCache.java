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

package com.ibm.stocator.fs.common.cache;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class adding an internal cache layer for objects metadata,
 * This cache is populated by the list function and on-the-fly requests for objects.
 */
public class ObjectCache {
  private HashMap<String, FileStatus> cache;
  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class);

  public ObjectCache() {
    cache = new HashMap<>();
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
  public FileStatus get(String objName) throws IOException {
    LOG.trace("Get from cache  {} ", objName);
    FileStatus res = cache.get(objName);
    return res;
  }

  public void put(String objNameKey, FileStatus fs) {
    LOG.trace("Add to cache  {} ", objNameKey);
    cache.put(objNameKey, fs);
  }

  public void remove(String objName) {
    LOG.trace("Remove from cache  {} ", objName);
    if (cache.containsKey(objName)) {
      cache.remove(objName);
    }
  }

  /**
   * removing the trailing slash because it is not supported in Swift
   * an request on an object (not a container) that has a trailing slash will lead
   * to a 404 response message
   */
  private String removeTrailingSlash(String objName) {
    String res = objName;
    if (res.endsWith("/")) {
      res = res.substring(0, res.length() - 1);
    }
    return res;
  }
}
