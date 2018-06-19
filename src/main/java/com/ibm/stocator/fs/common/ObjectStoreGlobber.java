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
 *  (C) Copyright IBM Corp. 2018
 */

package com.ibm.stocator.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStoreGlobber {
  public static final Logger LOG = LoggerFactory.getLogger(ObjectStoreGlobber.class.getName());

  private final ExtendedFileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;
  private final PathFilter filter;
  private final boolean bracketSupport;

  public ObjectStoreGlobber(ExtendedFileSystem fsT, Path pathPatternT, PathFilter filterT,
      boolean bracketSupportT) {
    fs = fsT;
    fc = null;
    pathPattern = pathPatternT;
    filter = filterT;
    bracketSupport = bracketSupportT;
  }

  public ObjectStoreGlobber(FileContext fcT, Path pathPatternT, PathFilter filterT,
      boolean bracketSupportT) {
    fs = null;
    fc = fcT;
    pathPattern = pathPatternT;
    filter = filterT;
    bracketSupport = bracketSupportT;
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

  public int getSpecialCharacter(String s) {
    if (s == null || s.trim().isEmpty()) {
      LOG.warn("Incorrect format of string {}", s);
      return 0;
    }
    Pattern p = Pattern.compile("[^A-Za-z0-9-_//:.+ =,']");
    Matcher m = p.matcher(s);
    boolean b = m.find();
    if (b == true) {
      LOG.trace("There is a special character in my string {} at position {}", s, m.start());
      return m.start();
    }
    return 0;
  }

  public FileStatus[] glob() throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    LOG.debug("Welcome to glob : " + pathPattern.toString());
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    String pathPatternString = pathPattern.toUri().getPath();
    String unescapePathString = unescapePathComponent(pathPatternString);
    int firstSpecialChar = getSpecialCharacter(unescapePathString);
    String noWildCardPathPrefix = unescapePathString.substring(0, firstSpecialChar);

    ArrayList<FileStatus> results = new ArrayList<>(1);
    ArrayList<FileStatus> candidates;
    ObjectStoreFlatGlobFilter globFilter = new ObjectStoreFlatGlobFilter(pathPattern.toString(),
        firstSpecialChar, bracketSupport);

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
      if (candidates.isEmpty()) {
        return new FileStatus[0];
      }
      for (FileStatus candidate : candidates) {
        if (filter.accept(candidate.getPath())
            && (candidate.getPath().toString().startsWith(pathPattern.toString() + "/")
                || (candidate.getPath().toString().equals(pathPattern.toString())))) {
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
