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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.ibm.stocator.fs.common.TestConstants;

import static com.ibm.stocator.fs.common.FileSystemTestUtils.assertListStatusFinds;
import static com.ibm.stocator.fs.common.FileSystemTestUtils.cleanup;
import static com.ibm.stocator.fs.common.FileSystemTestUtils.dumpStats;
import static com.ibm.stocator.fs.common.FileSystemTestUtils.touch;

/**
 * Test the FileSystem#listStatus() operations
 */
public class TestSwiftFileSystemLsOperations extends SwiftFileSystemBaseTest {

  private static Path[] sTestDirs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    createSwiftFileSystem();
    if (sFileSystem != null) {
      createTestSubdirs();
    }
  }

  /**
   * Create subdirectories and files under test/ for those tests
   * that want them. Doing so adds overhead to setup and teardown,
   * so should only be done for those tests that need them.
   * @throws IOException on an IO problem
   */
  private static void createTestSubdirs() throws IOException {

    sTestDirs = new Path[]{ new Path(sBaseURI + "/test/swift/a"),
                            new Path(sBaseURI + "/test/swift/b"),
                            new Path(sBaseURI + "/test/swift/c/1")};
    for (Path path : sTestDirs) {
      createEmptyFile(path);
    }
  }

  @Ignore("Not supported")
  public void testListLevelTest() throws Exception {
    FileStatus[] paths = sFileSystem.listStatus(path(getBaseURI() + "/test"));
    assertEquals(dumpStats(getBaseURI() + "/test", paths), 1, paths.length);
    assertEquals(path(getBaseURI() + "/test/swift"), paths[0].getPath());
  }

  @Ignore("Not supported")
  public void testListLevelTestSwift() throws Exception {
    FileStatus[] paths;
    paths = sFileSystem.listStatus(path(getBaseURI() + "/test/swift"));
    String stats = dumpStats("/test/swift", paths);
    assertEquals("Paths.length wrong in " + stats, 3, paths.length);
    assertEquals("Path element[0] wrong: " + stats, path(getBaseURI() + "/test/swift/a"),
                 paths[0].getPath());
    assertEquals("Path element[1] wrong: " + stats, path(getBaseURI() + "/test/swift/b"),
                 paths[1].getPath());
    assertEquals("Path element[2] wrong: " + stats, path(getBaseURI() + "/test/swift/c"),
                 paths[2].getPath());
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testListStatusEmptyDirectory() throws Exception {
    FileStatus[] paths;
    paths = sFileSystem.listStatus(path(getBaseURI() + "/test/swift/a"));
    assertEquals(dumpStats("/test/swift/a", paths), 0,
                 paths.length);
  }

  @Test(timeout = TestConstants.SWIFT_TEST_TIMEOUT)
  public void testListStatusFile() throws Exception {
    describe("Create a single file under /test;"
             + " assert that listStatus(/test) finds it");
    Path file = path(getBaseURI() + "/test/filename");
    createFile(file);
    FileStatus[] pathStats = sFileSystem.listStatus(file);
    assertEquals(dumpStats("/test/", pathStats),
                 1,
                 pathStats.length);
    //and assert that the len of that ls'd path is the same as the original
    FileStatus lsStat = pathStats[0];
    assertEquals("Wrong file len in listing of " + lsStat,
        data.length, lsStat.getLen());
  }

  public void testListEmptyRoot() throws Throwable {
    describe("Empty the root dir and verify that an LS / returns {}");
    cleanup("testListEmptyRoot", sFileSystem, "/test");
    cleanup("testListEmptyRoot", sFileSystem, "/user");
    FileStatus[] fileStatuses = sFileSystem.listStatus(path(getBaseURI() + "/"));
    assertEquals("Non-empty root" + dumpStats("/", fileStatuses),
                 0,
                 fileStatuses.length);
  }

  @Ignore("Unexpected")
  public void testListNonEmptyRoot() throws Throwable {
    Path test = path(getBaseURI() + "/test");
    touch(sFileSystem, test);
    FileStatus[] fileStatuses = sFileSystem.listStatus(path(getBaseURI() + "/"));
    String stats = dumpStats("/", fileStatuses);
    assertEquals("Wrong #of root children" + stats, 1, fileStatuses.length);
    FileStatus status = fileStatuses[0];
    assertEquals("Wrong path value" + stats,test, status.getPath());
  }

  @Ignore("Not Supported")
  public void testListStatusRootDir() throws Throwable {
    Path dir = path(getBaseURI() + "/");
    Path child = path(getBaseURI() + "/test");
    touch(sFileSystem, child);
    assertListStatusFinds(sFileSystem, dir, child);
  }

  @Ignore("Not supported")
  public void testListStatusFiltered() throws Throwable {
    Path dir = path(getBaseURI() + "/");
    Path child = path(getBaseURI() + "/test");
    touch(sFileSystem, child);
    FileStatus[] stats = sFileSystem.listStatus(dir, new AcceptAllFilter());
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(child)) {
        found = true;
      }
    }
    assertTrue("Path " + child
                      + " not found in directory " + dir + ":" + builder,
                      found);
  }

  /**
   * A path filter that accepts everything
   */
  private class AcceptAllFilter implements PathFilter {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  }
}
