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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Wrapper class adding an internal cache layer for objects metadata,
 * This cache is populated by the list function and on-the-fly requests for objects.
 *
 * Cache always keeps key and the cached fs without token if was provided as an input
 */
public class MemoryCache {
  private final Cache<String, FileStatus> fsCache;

  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(MemoryCache.class);
  private static MemoryCache sInstance;

  public static MemoryCache getInstance(int cacheSize) {
    if (sInstance == null) {
      sInstance = new MemoryCache(cacheSize);
    }
    return sInstance;
  }

  private MemoryCache(int cacheSize) {
    LOG.debug("Guava initiated with size {} expiration 30 seconds", cacheSize);
    fsCache = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterWrite(30, TimeUnit.SECONDS).build();
  }

  public void putFileStatus(String path, FileStatus fs) throws IOException {
    LOG.trace("Guava - add to cache {}", path);
    fsCache.put(path, new FileStatus(fs));
  }

  public void removeFileStatus(String path) {
    LOG.trace("Guava - remove from cache {}", path);
    fsCache.invalidate(path);
  }

  public FileStatus getFileStatus(final String path) {
    LOG.debug("Guava - get from cache {}", path);
    try {
      FileStatus cached =  fsCache.get(path, new Callable<FileStatus>() {
        @Override
        public FileStatus call() throws Exception {
          LOG.debug("Guava cache {} return null", path);
          return null;
        }
      });
      return new FileStatus(cached);
    } catch (Exception e) {
      LOG.debug("Guava - " + e.getMessage());
    }
    return null;
  }
}
