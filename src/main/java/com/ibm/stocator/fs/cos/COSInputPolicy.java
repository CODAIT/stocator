/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.cos;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADV_NORMAL;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADV_SEQUENTIAL;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADV_RANDOM;
import static com.ibm.stocator.fs.cos.COSConstants.INPUT_FADVISE;
/**
 * Filesystem input policy.
 */

public enum COSInputPolicy {

  Normal(INPUT_FADV_NORMAL),
  Sequential(INPUT_FADV_SEQUENTIAL),
  Random(INPUT_FADV_RANDOM);

  private static final Logger LOG =
      LoggerFactory.getLogger(COSInputPolicy.class);
  private final String policy;

  COSInputPolicy(String policyT) {
    policy = policyT;
  }

  @Override
  public String toString() {
    return policy;
  }

  /**
   * Choose an FS access policy.
   * Always returns something,
   * primarily by downgrading to "normal" if there is no other match.
   * @param name strategy name from a configuration option, etc
   * @return the chosen strategy
   */
  public static COSInputPolicy getPolicy(String name) {
    String trimmed = name.trim().toLowerCase(Locale.ENGLISH);
    switch (trimmed) {
      case INPUT_FADV_NORMAL:
        return Normal;
      case INPUT_FADV_RANDOM:
        return Random;
      case INPUT_FADV_SEQUENTIAL:
        return Sequential;
      default:
        LOG.warn("Unrecognized " + INPUT_FADVISE + " value: \"{}\"", trimmed);
        return Normal;
    }
  }

}
