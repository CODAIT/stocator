package com.ibm.stocator.fs.swift2d.systemtests;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.ibm.stocator.fs.swift2d.systemtests.SwiftTestUtils.readBytesToString;
import static com.ibm.stocator.fs.swift2d.systemtests.SwiftTestUtils.writeTextFile;

public class TestSwiftSpecialChars extends SwiftFileSystemBaseTest {

  @BeforeClass
  public static void setUpClass() throws Exception {
    createSwiftFileSystem("container test");
  }

  @Test
  public void testContainerNamesWithSpaces() throws Throwable {
    Path path = new Path(getBaseURI() + "/test/object Name");
    String text = "Testing PUT and GET on container with spaces "
            + System.currentTimeMillis();
    writeTextFile(sFileSystem, path, text, false);
    String result = readBytesToString(sFileSystem, path, text.length());
    assertEquals(text, result);
  }
}
