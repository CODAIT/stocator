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

package com.ibm.stocator.fs.cache;

import java.util.Date;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class adding an internal cache layer for objects metadata,
 * This cache is populated by the list function and on-the-fly requests for objects.
 */
public class MemoryCache {
  private HashMap<String, CachedObject> cache;
  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(MemoryCache.class);
  private static MemoryCache sInstance;

  public static MemoryCache getInstance() {
    if (sInstance == null) {
      sInstance = new MemoryCache();
    }
    return sInstance;
  }

  private MemoryCache() {
    cache = new HashMap<>();
  }

  /**
   * The get function will first search for the object in the cache.
   * If not found will issue a HEAD request for the object metadata
   * and add the object to the cache.
   *
   * @param objName object name
   * @return cached entry of the object
   */
  public CachedObject get(String objName) {
    LOG.trace("Get from cache  {} ", objName);
    CachedObject res = cache.get(objName);
    return res;
  }

  public void put(String objNameKey, long contentLength, Date lastModified) {
    LOG.trace("Add to cache  {} ", objNameKey);
    CachedObject co = new CachedObject(contentLength, lastModified);
    cache.put(objNameKey, co);
  }

  public void remove(String objName) {
    LOG.trace("Remove from cache  {} ", objName);
    if (cache.containsKey(objName)) {
      cache.remove(objName);
    }
  }
}
