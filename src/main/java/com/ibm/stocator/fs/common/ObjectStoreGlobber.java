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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStoreGlobber {
  public static final Logger LOG = LoggerFactory.getLogger(ObjectStoreGlobber.class.getName());

  private final FileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;
  private final PathFilter filter;

  public ObjectStoreGlobber(FileSystem fsT, Path pathPatternT, PathFilter filterT) {
    fs = fsT;
    fc = null;
    pathPattern = pathPatternT;
    filter = filterT;
  }

  public ObjectStoreGlobber(FileContext fcT, Path pathPatternT, PathFilter filterT) {
    fs = null;
    fc = fcT;
    pathPattern = pathPatternT;
    filter = filterT;
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  private FileStatus[] listStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  /**
   * Convert a path component that contains backslash ecape sequences to a
   * literal string. This is necessary when you want to explicitly refer to a
   * path that contains globber metacharacters.
   */
  private static String unescapePathComponent(String name) {
    return name.replaceAll("\\\\(.)", "$1");
  }

  /**
   * Translate an absolute path into a list of path components. We merge double
   * slashes into a single slash here. POSIX root path, i.e. '/', does not get
   * an entry in the list.
   */
  private static List<String> getPathComponents(String path) throws IOException {
    ArrayList<String> ret = new ArrayList<String>();
    for (String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private String schemeFromPath(Path path) throws IOException {
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      }
    }
    return scheme;
  }

  private String authorityFromPath(Path path) throws IOException {
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      }
    }
    return authority;
  }

  public FileStatus[] glob() throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    LOG.debug("Welcome to glob : " + pathPattern.toString());
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs. Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    String pathPatternString = pathPattern.toUri().getPath();
    List<String> flattenedPatterns = ObjectStoreGlobExpander.expand(pathPatternString);

    LOG.debug("expanded : " + pathPatternString);
    // Now loop over all flattened patterns. In every case, we'll be trying to
    // match them to entries in the filesystem.
    ArrayList<FileStatus> results = new ArrayList<FileStatus>(flattenedPatterns.size());
    boolean sawWildcard = false;
    for (String flatPattern : flattenedPatterns) {
      LOG.debug("pattern from list: " + flatPattern);
      Path absPattern = new Path(flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern);
      List<String> components = getPathComponents(absPattern.toUri().getPath());
      ArrayList<FileStatus> candidates = new ArrayList<FileStatus>(1);
      FileStatus rootPlaceholder = new FileStatus(0, true, 0, 0, 0,
          new Path(scheme, authority, Path.SEPARATOR));
      LOG.debug("Going to add candidate: " + rootPlaceholder.getPath().toString());
      candidates.add(rootPlaceholder);
      String cmpCombined = "";
      ObjectStoreGlobFilter globFilter = null;
      for (int componentIdx = 0; componentIdx < components.size() && !sawWildcard;
          componentIdx++) {
        globFilter = new ObjectStoreGlobFilter(components.get(componentIdx));
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        } else {
          cmpCombined = cmpCombined + "/" + components.get(componentIdx);
        }
      }
      String component = unescapePathComponent(cmpCombined);
      if (component != null && component.length() > 0) {
        for (FileStatus candidate : candidates) {
          candidate.setPath(new Path(candidate.getPath(), component));
        }
      } else {
        globFilter = new ObjectStoreGlobFilter(components.get(0));
      }
      ArrayList<FileStatus> newCandidates = new ArrayList<FileStatus>(candidates.size());
      for (FileStatus candidate : candidates) {
        if (globFilter.hasPattern()) {
          FileStatus[] children = listStatus(candidate.getPath());
          if (children.length == 1) {
            if (!getFileStatus(candidate.getPath()).isDirectory()) {
              continue;
            }
          }
          for (FileStatus child : children) {
            if (globFilter.accept(child.getPath())) {
              newCandidates.add(child);
            }
          }
        } else {
          FileStatus childStatus = null;
          childStatus = getFileStatus(new Path(candidate.getPath(), component));
          if (childStatus != null) {
            newCandidates.add(childStatus);
          }
        }
      }
      candidates = newCandidates;
      for (FileStatus status : candidates) {
        if (status == rootPlaceholder) {
          status = getFileStatus(rootPlaceholder.getPath());
          if (status == null) {
            continue;
          }
        }
        if (filter.accept(status.getPath())) {
          results.add(status);
        }
      }
    }
    if (!sawWildcard && results.isEmpty() && (flattenedPatterns.size() <= 1)) {
      return null;
    }
    return results.toArray(new FileStatus[0]);
  }
}
