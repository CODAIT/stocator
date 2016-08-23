package com.ibm.stocator.fs.common;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public abstract class ExtendedFileSystem extends FileSystem {

  public abstract FileStatus[] listStatus(Path f,
      PathFilter filter, boolean prefixBased) throws FileNotFoundException, IOException;

}
