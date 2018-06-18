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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStoreFlatGlobFilter implements PathFilter{

  public static final Logger LOG = LoggerFactory.getLogger(
      ObjectStoreFlatGlobFilter.class.getName());

  private String pathPattern;
  private String prefix;
  private int start;

  public ObjectStoreFlatGlobFilter(String pathPattern1, String prefix1, int start1) {
    prefix = prefix1;
    pathPattern = pathPattern1;
    start = start1;
  }

  @Override
  public boolean accept(Path path) {
    LOG.trace("accept on {}, path pattern {}", path.toString(), pathPattern);
    String name = path.getName();
    if (name != null && name.startsWith("part-") && name.contains("attempt_")) {
      LOG.trace("accept on parent {}, path pattern {}", path.getParent().toString(), pathPattern);
      return FilenameUtils.wildcardMatch(path.getParent().toString(), pathPattern);
    }
    return FilenameUtils.wildcardMatch(path.toString(), pathPattern);
  }

  public boolean hasPattern() {
    return (start > 0);
  }
}
