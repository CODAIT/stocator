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

package com.ibm.stocator.fs.common;

import java.util.regex.PatternSyntaxException;
import java.io.IOException;

import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A filter for POSIX glob pattern with brace expansions.
 */
public class ObjectStoreGlobFilter implements PathFilter {
  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
      @Override
      public boolean accept(Path file) {
        return true;
      }
    };

  private PathFilter userFilter = DEFAULT_FILTER;
  private GlobPattern pattern;

  /**
   * Creates a glob filter with the specified file pattern.
   *
   * @param filePattern the file pattern
   * @throws IOException thrown if the file pattern is incorrect
   */
  public ObjectStoreGlobFilter(String filePattern) throws IOException {
    init(filePattern, DEFAULT_FILTER);
  }

  /**
   * Creates a glob filter with the specified file pattern and an user filter.
   *
   * @param filePattern the file pattern
   * @param filter user filter in addition to the glob pattern
   * @throws IOException thrown if the file pattern is incorrect
   */
  public ObjectStoreGlobFilter(String filePattern, PathFilter filter) throws IOException {
    init(filePattern, filter);
  }

  void init(String filePattern, PathFilter filter) throws IOException {
    try {
      userFilter = filter;
      pattern = new GlobPattern(filePattern);
    } catch (PatternSyntaxException e) {
      throw new IOException("Illegal file pattern: " + e.getMessage(), e);
    }
  }

  boolean hasPattern() {
    return pattern.hasWildcard();
  }

  @Override
  public boolean accept(Path path) {
    return pattern.matches(path.toString()) && userFilter.accept(path);
  }
}
