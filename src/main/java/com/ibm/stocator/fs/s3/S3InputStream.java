package com.ibm.stocator.fs.s3;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSInputStream;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

public class S3InputStream extends FSInputStream {

  private S3Object s3Obj;
  private S3Service s3Service;
  private InputStream in;
  private long pos = 0;

  S3InputStream(S3Object obj, S3Service s3) {
    s3Service = s3;
    s3Obj = obj;
    try {
      in = obj.getDataInputStream();
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized void seek(long l) throws IOException {
    if (l < 0) {
      throw new EOFException("Position can not be negative.");
    }
    pos = l;
    try {
      StorageObject details = s3Service.getObjectDetails(s3Obj.getBucketName(), s3Obj.getName());
      if (details.getContentLength() < l) {
        throw new EOFException("Seeking Past End of File");
      }
      s3Obj = s3Service.getObject(s3Obj.getBucketName(),
          s3Obj.getName(), null, null, null, null, l, null);
      in = s3Obj.getDataInputStream();
    } catch (ServiceException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    return this.pos;
  }

  @Override
  public synchronized boolean seekToNewSource(long l) throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    return in.read();
  }

  @Override
  public synchronized int read(byte[] b, int i, int len) throws IOException {
    return in.read(b, i, len);
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      if (in != null) {
        in.close();
      }
    } finally {
      in = null;
    }
  }
}
