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

package com.ibm.stocator.fs.cos;

import java.io.File;
import java.io.IOException;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.commons.codec.digest.DigestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extend LocalDirAllocator (An implementation of a round-robin scheme for
 * disk allocation for creating file) to avoid callign File.createTempFile().
 * We have found that createTempFile() results in slower runs on some machines
 * for reasons that are not clear to us.  Possibly this has to do with the
 * need to ensure that a file with the same name does not already exists.
 * We have replaced createTempFile() with code that generates a unique file
 * name using the object name, partition number, task number, attempt number,
 * time stamp, and finally a uuid number.
 */
public class COSLocalDirAllocator extends LocalDirAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(COSAPIClient.class);
  private Configuration conf;
  public static final int SIZE_UNKNOWN = -1;

  public COSLocalDirAllocator(Configuration pConf, String bufferDir) {
    super(bufferDir);
    conf = pConf;
  }

  private synchronized File createTmpDirForWrite(String tmpDirName) throws IOException {
    LOG.trace("tmpDirName is {}", tmpDirName);
    File tmpDir = new File(tmpDirName);
    if (!tmpDir.exists()) {
      tmpDir.mkdir();
    }
    return tmpDir;
  }

  public File createTmpFileForWrite(String pathStr, long size,
      Configuration conf) throws IOException {
    String sha256hex = DigestUtils.sha256Hex(pathStr);
    Path path = getLocalPathForWrite(sha256hex, size, conf, true);
    File tmpDir = new File(path.getParent().toUri().getPath());
    File tmpFile = new File(tmpDir, sha256hex + UUID.randomUUID().toString());
    return tmpFile;
  }
}
