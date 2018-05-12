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
 */

package com.ibm.stocator.fs.swift2d.systemtests;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.ibm.stocator.fs.common.FileSystemTestUtils;
import com.ibm.stocator.fs.common.TestConstants;

/**
 * Tests that blocksize is never zero for a file, either in the FS default
 * or the FileStatus value of a queried file
 */
public class TestSwiftFileSystemBlocksize extends SwiftFileSystemBaseTest {

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDefaultBlocksizeNonZero() throws Throwable {
    assertTrue("Zero default blocksize", 0L != getFs().getDefaultBlockSize());
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDefaultBlocksizeRootPathNonZero() throws Throwable {
    assertTrue("Zero default blocksize",
               0L != getFs().getDefaultBlockSize(new Path(getBaseURI() + "/")));
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testDefaultBlocksizeOtherPathNonZero() throws Throwable {
    assertTrue("Zero default blocksize",
               0L != getFs().getDefaultBlockSize(new Path(getBaseURI() + "/test")));
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testBlocksizeNonZeroForFile() throws Throwable {
    Path smallfile = new Path(getBaseURI() + "/test/smallfile");
    FileSystemTestUtils.writeTextFile(sFileSystem, smallfile, "blocksize", true);
    createFile(smallfile);
    FileStatus status = getFs().getFileStatus(smallfile);
    assertTrue("Zero blocksize in " + status,
               status.getBlockSize() != 0L);
    assertTrue("Zero replication in " + status,
               status.getReplication() != 0L);
  }
}
