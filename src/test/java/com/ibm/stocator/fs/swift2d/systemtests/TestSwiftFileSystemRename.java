package com.ibm.stocator.fs.swift2d.systemtests;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test FileSystem rename/move operations
 */
public class TestSwiftFileSystemRename extends SwiftFileSystemBaseTest {

  @Test
  public void singleObjectRename() throws IOException {
    Path srcPath = new Path(getBaseURI() + "/Dir/SubDir/A");
    Path dstPath = new Path(getBaseURI() + "/Dir/SubDir/B");
    createFile(srcPath);
    sFileSystem.rename(srcPath, dstPath);

    Assert.assertTrue(sFileSystem.exists(dstPath));
  }

  @Test
  public void multipleObjectRename() throws IOException {
    Path srcPath = new Path(getBaseURI() + "/Dir/SubDir");
    String[] sources = {"/Dir/SubDir/A", "Dir/SubDir/B", "Dir/SubDir/C"};

    createEmptyFile(srcPath);
    for (String source : sources) {
      createFile(new Path(getBaseURI() + "/" + source));
    }
    Path dstPath = new Path(getBaseURI() + "/Dir/NewSubDir");
    sFileSystem.rename(srcPath, dstPath);

    Assert.assertTrue(sFileSystem.exists(dstPath));
    for (String source : sources) {
      Path newPath = new Path(getBaseURI() + "/" + source.replace("SubDir", "NewSubDir"));
      Assert.assertTrue(sFileSystem.exists(newPath));
    }
  }
}
