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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.PathOperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStoreFlatGlobFilter implements PathFilter{

  public static final Logger LOG = LoggerFactory.getLogger(
      ObjectStoreFlatGlobFilter.class.getName());

  private ArrayList<String> parsedPatterns;
  private int start;
  private boolean bracketSupport;

  public ObjectStoreFlatGlobFilter(String pathPattern1, int start1, boolean bracketSupport1)
      throws PathOperationException {
    bracketSupport = bracketSupport1;
    parsedPatterns = parseInnerSet(pathPattern1);
    start = start1;
  }

  private ArrayList<String> parseInnerSet(String pathStr) throws PathOperationException {
    LOG.trace("Process {}", pathStr);
    ArrayList<String> result = new ArrayList<String>();
    // found start and finish of the external boundary { }
    // we currently support only one nested level
    int nestedLevel = 0;
    Queue<String> q = new LinkedList<String>();
    int fromIndex = pathStr.indexOf("{");
    if (!bracketSupport || fromIndex < 0) {
      result.add(pathStr);
      return result;
    }
    int currIndex = fromIndex + 1;
    if (fromIndex >= 0) {
      q.add("{");
      nestedLevel++;
      while (!q.isEmpty() && currIndex < pathStr.length()) {
        if (pathStr.charAt(currIndex) == '{') {
          q.add("{");
          nestedLevel++;
        } else if (pathStr.charAt(currIndex) == '}') {
          q.remove();
        }
        currIndex++;
      }
    }
    if (nestedLevel > 2) {
      LOG.error("Only 1 nested level allowed. Found {} for {}", nestedLevel, pathStr);
      throw new PathOperationException("Only one nested level of brackets is supported");
    }
    if (!q.isEmpty()) {
      LOG.error("Invalid input {}", pathStr);
      throw new PathOperationException("Invalid input " + pathStr);

    }
    String globalPrefix = pathStr.substring(0, fromIndex);
    LOG.trace("Global prefix {} for {}", globalPrefix, pathStr);
    String globalSuffix = pathStr.substring(currIndex);
    LOG.trace("Global suffix {} for {}", globalSuffix, pathStr);

    // list parsed path inside external boundary
    // we need to make sure we don't separate inner boundaries
    StringTokenizer strk = new StringTokenizer(pathStr.substring(fromIndex + 1,
        currIndex - 1), ",");
    while (strk.hasMoreTokens()) {
      String st = strk.nextToken();
      // for each value see if internal boundary present, xx{yy,zz}ww
      int startInd = st.indexOf("{");
      int endInd = st.indexOf("}");
      // means it break inner boundary, need to aggregate
      while (endInd < 0 && startInd > 0) {
        st = st + "," + strk.nextToken();
        endInd = st.indexOf("}");
      }
      if (startInd >= 0 && endInd <= st.length()) {
        String localPrefix = st.substring(0, startInd);
        String localSuffix = st.substring(endInd + 1, st.length());
        LOG.trace("Local prefix: {} :local suffix {} for {}", localPrefix,
            localSuffix, st);
        StringTokenizer localTokenizer = new StringTokenizer(st.substring(
            startInd + 1, endInd), ",");
        while (localTokenizer.hasMoreTokens()) {
          String entry = localTokenizer.nextToken();
          result.add(globalPrefix + localPrefix + entry
              + localSuffix + globalSuffix);
        }
      } else {
        result.add(globalPrefix + st + globalSuffix);
      }
    }
    return result;
  }

  @Override
  public boolean accept(Path path) {
    String name = path.getName();
    String pathStr = path.toString();
    boolean match = true;
    for (String pathPattern : parsedPatterns) {
      LOG.trace("accept on {}, path pattern {}, name {}", pathStr, pathPattern, name);
      if (name != null && name.startsWith("part-")) {
        LOG.trace("accept on parent {}, path pattern {}",
            path.getParent().toString(), pathPattern);
        match = FilenameUtils.wildcardMatch(path.getParent().toString(), pathPattern);
      } else {
        match = FilenameUtils.wildcardMatch(pathStr, pathPattern);
      }
      if (match) {
        return match;
      }
    }
    return match;
  }

  public boolean hasPattern() {
    return (start > 0);
  }
}
