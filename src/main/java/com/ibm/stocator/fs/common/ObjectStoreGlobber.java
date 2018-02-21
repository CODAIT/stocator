/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  (C) Copyright IBM Corp. 2015, 2016
 */

package com.ibm.stocator.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.cos.COSUtils;

public class ObjectStoreGlobber {
  public static final Logger LOG = LoggerFactory.getLogger(ObjectStoreGlobber.class.getName());

  private final ExtendedFileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;
  private final PathFilter filter;

  public ObjectStoreGlobber(ExtendedFileSystem fsT, Path pathPatternT, PathFilter filterT) {
    this(null, fsT, pathPatternT, filterT);
  }

  public ObjectStoreGlobber(FileContext fcT, Path pathPatternT, PathFilter filterT) {
    this(fcT, null, pathPatternT, filterT);
  }

  private ObjectStoreGlobber(FileContext fcT, ExtendedFileSystem fsT, Path pathPatternT,
                PathFilter filterT) {
    pathPattern = pathPatternT;
    fs = fsT;
    fc = fcT;
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

  private FileStatus[] listStatus(Path path, boolean isDirectory) throws IOException {
    try {
      if (fs != null) {
        //return fs.listStatus(new Path(path.toString() + "*"));
        return fs.listStatus(path, filter, true, isDirectory);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (FileNotFoundException e) {
      return new FileStatus[0];
    }
  }

  /**
   * Convert a path component that contains backslash escape sequences to a
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

  private String getPrefixUpToFirstWildcard(String path) {
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '*' || c == '{' || c == '[' || (c == '?'
          && !path.substring(i, i + 7).equals("?token="))) { // discard ?token=
        return path.substring(0, i);
      }
    }
    return path;
  }

  public FileStatus[] glob() throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    LOG.debug("Welcome to glob : " + pathPattern.toString());
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    String pathPatternString = pathPattern.toUri().getPath();
    String unescapePathString = unescapePathComponent(pathPatternString);
    String noWildCardPathPrefix = getPrefixUpToFirstWildcard(unescapePathString);

    ArrayList<FileStatus> results = new ArrayList<>(1);
    ArrayList<FileStatus> candidates;
    String prefix = new Path(fs.getHostnameScheme() + noWildCardPathPrefix).toString();
    if (!prefix.endsWith("/")) {
      prefix = prefix + "/";
    }
    // remove ?token from globFilter
    ObjectStoreGlobFilter globFilter = new ObjectStoreGlobFilter(
          COSUtils.removeToken(pathPattern.toString()), COSUtils.removeToken(prefix));

    if (pathPatternString.contains("?temp_url")) {
      FileStatus[] fs = {getFileStatus(pathPattern)};
      return fs;
    }

    if (globFilter.hasPattern()) {
      // Get a list of FileStatuses and filter
      LOG.trace("Glob filter {} no wildcard prefix {}", pathPatternString, noWildCardPathPrefix);
      FileStatus rootPlaceholder = new FileStatus(0, true, 0, 0, 0,
              new Path(scheme, authority, Path.SEPARATOR + noWildCardPathPrefix));
      LOG.trace("Glob filter {} pattern {}", rootPlaceholder.getPath(),
          pathPatternString.toString());
      String pathToList = rootPlaceholder.getPath().toString();
      candidates = new ArrayList<>(Arrays.asList(listStatus(rootPlaceholder.getPath(),
          noWildCardPathPrefix.endsWith("/"))));
      for (FileStatus candidate : candidates) {
        if (globFilter.accept(candidate.getPath())) {
          LOG.trace("Candidate accepted: {}", candidate.getPath().toString());
          results.add(candidate);
        } else {
          LOG.trace("Candidate rejected: {} Pattern {}", candidate.getPath().toString(),
              rootPlaceholder.getPath());
        }
      }
    } else {
      LOG.debug("No globber pattern. Get a single FileStatus based on path given {}",
          pathPattern.toString());
      candidates = new ArrayList<>(Arrays.asList(getFileStatus(new Path(pathPattern.toString()))));
      if (candidates == null || candidates.isEmpty()) {
        return new FileStatus[0];
      }
      LOG.debug("About to loop over candidates");
      for (FileStatus candidate : candidates) {
        if (candidate == null) {
          throw new FileNotFoundException("Not found " + pathPatternString);
        }
        LOG.trace("Loop over {}", candidate);
        // if ?token exists then compare to pattern without the ?token=
        if (filter.accept(candidate.getPath())
            && (candidate.getPath().toString().startsWith(
                COSUtils.removeToken(pathPattern.toString()) + "/")
                || (candidate.getPath().toString().equals(
                    COSUtils.removeToken(pathPattern.toString()))))) {
          results.add(candidate);
        }
      }
    }
    if (results.isEmpty()) {
      return new FileStatus[0];
    }
    return results.toArray(new FileStatus[0]);
  }
}
