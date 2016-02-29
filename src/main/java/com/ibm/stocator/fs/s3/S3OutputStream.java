/**
 * (C) Copyright IBM Corp. 2015, 2016
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.s3;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps OutputStream
 * This class is not thread-safe
 */
public class S3OutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(S3OutputStream.class);

  private static final int STREAMING_CHUNK = 8 * 1024 * 1024;

  private static final int READ_TIMEOUT = 100 * 1000;
  private final S3Service s3;
  private final S3Object s3obj;

  private ByteArrayOutputStream mOutputStream;

  public S3OutputStream(S3Object obj, S3Service service) throws IOException, ServiceException {
    s3 = service;
    s3obj = obj;
    mOutputStream = new ByteArrayOutputStream();
  }

  @Override
  public void write(int b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOutputStream.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOutputStream.write(b);
  }

  @Override
  public void close() throws IOException {
    byte[] buf = mOutputStream.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(buf);
    s3obj.setContentLength(buf.length);
    s3obj.setDataInputStream(in);

    try {
      s3.putObject(s3obj.getBucketName(), s3obj);
    } catch (S3ServiceException e) {
      System.out.println(e.getMessage());
    }
    mOutputStream.close();
  }

  @Override
  public void flush() throws IOException {
    mOutputStream.flush();
  }
}
