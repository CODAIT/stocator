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

package com.ibm.stocator.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.internal.AssumptionViolatedException;

/**
 * Utilities used across test cases
 */
public class FileSystemTestUtils extends org.junit.Assert {

  private static final Log LOG =
              LogFactory.getLog(FileSystemTestUtils.class);

  /**
   * Read an object from FileSystem
   *
   * @param fs filesystem
   * @param path object bath
   * @param len how much to read
   * @return byte array
   * @throws IOException if can't read an object
   */
  public static byte[] readDataset(FileSystem fs, Path path, int len) throws IOException {
    FSDataInputStream in = fs.open(path);
    byte[] dest = new byte[len];
    try {
      in.readFully(0, dest);
    } finally {
      in.close();
    }
    return dest;
  }

  /**
   * Random dataset
   *
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   * @return the newly generated dataset
   */
  public static byte[] generateDataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathExists(FileSystem fileSystem, String message,
                                      Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      fail(message + ": not found " + path + " in " + path.getParent());
    }
  }

  /**
   * Assert that a path does not exist
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(FileSystem fileSystem,
                                            String message,
                                            Path path) throws IOException {
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      if (status != null) {
        fail(message + ": unexpectedly found " + path + " as  " + status);
      }
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  public static void assertDeleted(FileSystem fs,
                                   Path file,
                                   boolean recursive) throws IOException {
    assertPathExists(fs, "about to be deleted file", file);
    fs.delete(file, recursive);
    assertPathDoesNotExist(fs, "Deleted file", file);
  }

  public static void noteAction(String action) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("==============  " + action + " =============");
    }
  }

  /**
   * Write the text to a file, returning the converted byte array
   * for use in validating the round trip
   * @param fs filesystem
   * @param path path of file
   * @param text text to write
   * @param overwrite should the operation overwrite any existing file?
   * @return the read bytes
   * @throws IOException on IO problems
   */
  public static byte[] writeTextFile(FileSystem fs,
                                     Path path,
                                     String text,
                                     boolean overwrite) throws IOException {
    FSDataOutputStream stream = fs.create(path, overwrite);
    byte[] bytes = new byte[0];
    if (text != null) {
      bytes = toAsciiByteArray(text);
      stream.write(bytes);
    }
    stream.close();
    return bytes;
  }

  /**
   * Touch a file: fails if it is already there
   * @param fs filesystem
   * @param path path
   * @throws IOException IO problems
   */
  public static void touch(FileSystem fs,
                           Path path) throws IOException {
    fs.delete(path, true);
    writeTextFile(fs, path, null, false);
  }

  public static byte[] toAsciiByteArray(String s) {
    char[] chars = s.toCharArray();
    int len = chars.length;
    byte[] buffer = new byte[len];
    for (int i = 0; i < len; i++) {
      buffer[i] = (byte) (chars[i] & 0xff);
    }
    return buffer;
  }

  /**
   * Read in "length" bytes, convert to an ascii string
   * @param fs filesystem
   * @param path path to read
   * @param length #of bytes to read
   * @return the bytes read and converted to a string
   * @throws IOException
   */
  public static String readBytesToString(FileSystem fs,
                                         Path path,
                                         int length) throws IOException {
    FSDataInputStream in = fs.open(path);
    try {
      byte[] buf = new byte[length];
      in.readFully(0, buf);
      return toChar(buf);
    } finally {
      in.close();
    }
  }

  public static String toChar(byte[] buffer) {
    StringBuilder builder = new StringBuilder(buffer.length);
    for (byte b : buffer) {
      builder.append(toChar(b));
    }
    return builder.toString();
  }

  /**
   * Convert a byte to a character for printing. If the
   * byte value is &lt; 32 -and hence unprintable- the byte is
   * returned as a two digit hex value
   * @param b byte
   * @return the printable character string
   */
  public static String toChar(byte b) {
    if (b >= 0x20) {
      return Character.toString((char) b);
    } else {
      return String.format("%02x", b);
    }
  }

  /**
   * Make an assertion about the length of a file
   * @param fs filesystem
   * @param path path of the file
   * @param expected expected length
   * @throws IOException on File IO problems
   */
  public static void assertFileHasLength(FileSystem fs, Path path,
                                         int expected) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    assertEquals(
            "Wrong file length of file " + path + " status: " + status,
            expected,
            status.getLen());
  }

  /**
   * Assert that a path refers to a directory
   * @param fs filesystem
   * @param path path of the directory
   * @throws IOException on File IO problems
   */
  public static void assertIsDirectory(FileSystem fs,
                                       Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertIsDirectory(fileStatus);
  }

  /**
   * Assert that a path refers to a directory
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a dir -but isn't: " + fileStatus,
            fileStatus.isDirectory());
  }

  /**
   * downgrade a failure to a message and a warning, then an
   * exception for the Junit test runner to mark as failed
   * @param message text message
   * @param failure what failed
   * @throws AssumptionViolatedException always
   */
  public static void downgrade(String message, Throwable failure) {
    LOG.warn("Downgrading test " + message, failure);
    AssumptionViolatedException ave =
            new AssumptionViolatedException(failure, null);
    throw ave;
  }

  public static String dumpStats(String pathname, FileStatus[] stats) {
    return pathname + fileStatsToString(stats,"\n");
  }

  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

  public static void cleanup(String action,
                             FileSystem fileSystem,
                             String cleanupPath) {
    noteAction(action);
    try {
      if (fileSystem != null) {
        fileSystem.delete(new Path(cleanupPath).makeQualified(fileSystem),
                true);
      }
    } catch (Exception e) {
      LOG.error("Error deleting in " + action + " - "  + cleanupPath + ": " + e, e);
    }
  }

  /**
   * Deletes all files in a container
   * @param fileSystem
   * @param BaseUri
   * @throws IOException
     */
  public static void cleanupAllFiles(FileSystem fileSystem, String baseUri) throws IOException {
    try {
      if (fileSystem != null) {
        // Clean up generated files
        /*
        Path rootDir = new Path(BaseUri + "/");
        FileStatus[] files = fileSystem.listStatus(rootDir);
        for (FileStatus file : files) {
          System.out.println("Cleanup of : " + file.getPath());
          fileSystem.delete(file.getPath(), false);
        }
      }
      */
        Path rootDir = new Path(baseUri + "/");
        fileSystem.delete(rootDir, true);
      }
    } catch (Exception e) {
      LOG.error("Error in deleting all files.");
    }
  }

  /**
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry
   * @param fs filesystem
   * @param dir directory to scan
   * @param subdir full path to look for
   * @throws IOException IO problems
   */
  public static void assertListStatusFinds(FileSystem fs,
                                           Path dir,
                                           Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
                    + " not found in directory " + dir + ":" + builder,
            found);
  }

}
