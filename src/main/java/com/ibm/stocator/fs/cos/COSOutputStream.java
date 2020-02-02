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

import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.transfer.Upload;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.transfer.TransferManager;
import com.ibm.cloud.objectstorage.AmazonClientException;

public class COSOutputStream extends OutputStream {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(COSOutputStream.class);
  /*
   * Name of the bucket the object resides in
   */
  private final String mBucketName;

  /*
   * The path of the object to read
   */
  private final String mKey;

  /*
   * Object`s content type
   */
  private String mContentType;

  /*
   * Object`s metadata
   */
  private Map<String, String> mMetadata;
  /*
   * Output stream
   */
  private final OutputStream mBackupOutputStream;
  /*
   * Temporal output file (output stream is buffered by it)
   */
  private final File mBackupFile;
  /*
   * Closed bit. Updates must be in a synchronized block to guarantee an atomic
   * check and set
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);
  /*
   * Transfer manager
   */
  private TransferManager transfers;
  /*
   * COSAPIclient
   */
  private final COSAPIClient fs;

  /**
   * Object's etag for atomic write
   */
  private final String mEtag;

  /**
   * Indicates whether the PutObjectRequest will be atomic or not
   */
  private Boolean mAtomicWriteEnabled;

  /**
   * Constructor for an output stream of an object in COS
   *
   * @param bucketName the bucket the object resides in
   * @param key the key of the object to read
   * @param client the COS client to use for operations
   * @param contentType the content type written to the output stream
   * @param metadata the object`s metadata
   * @param transfersT TransferManager
   * @param etag the etag to be used in atomic write (null if no etag exists)
   * @param atomicWriteEnabled if true the putObject
   * @param fsT COSAPIClient
   *
   * @throws IOException if error
   */
  public COSOutputStream(String bucketName, String key, AmazonS3 client, String contentType,
      Map<String, String> metadata, TransferManager transfersT,
      COSAPIClient fsT, String etag, Boolean atomicWriteEnabled) throws IOException {
    mBucketName = bucketName;
    mEtag = etag;
    mAtomicWriteEnabled = atomicWriteEnabled;
    transfers = transfersT;
    fs = fsT;
    // Remove the bucket name prefix from key path
    if (key.startsWith(bucketName + "/")) {
      mKey = key.substring(bucketName.length() + 1);
    } else {
      mKey = key;
    }
    mContentType = contentType;
    mMetadata = metadata;
    try {
      String tmpPrefix = (key.replaceAll("/", "-")).replaceAll(":", "-");
      mBackupFile = fs.createTmpFileForWrite("output-" + tmpPrefix);
      LOG.trace("OutputStream for key '{}' writing to tempfile: {}", key, mBackupFile);
      mBackupOutputStream = new BufferedOutputStream(new FileOutputStream(mBackupFile), 32768);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw e;
    }
  }

  void checkOpen() throws IOException {
    if (closed.get()) {
      throw new IOException("Output Stream closed");
    }
  }

  @Override
  public void write(int b) throws IOException {
    checkOpen();
    mBackupOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkOpen();
    mBackupOutputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    checkOpen();
    mBackupOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      return;
    }
    mBackupOutputStream.close();
    LOG.debug("OutputStream for key '{}' closed. Now beginning upload", mKey);
    try {
      final ObjectMetadata om = new ObjectMetadata();
      om.setContentLength(mBackupFile.length());
      om.setContentType(mContentType);
      om.setUserMetadata(mMetadata);
      // if atomic write is enabled use the etag to ensure put request is atomic
      if (mAtomicWriteEnabled) {
        if (mEtag != null) {
          LOG.debug("Atomic write - setting If-Match header");
          om.setHeader("If-Match", mEtag);
        } else {
          LOG.debug("Atomic write - setting If-None-Match header");
          om.setHeader("If-None-Match", "*");
        }
      }

      PutObjectRequest putObjectRequest = new PutObjectRequest(mBucketName, mKey, mBackupFile);
      putObjectRequest.setMetadata(om);

      Upload upload = transfers.upload(putObjectRequest);

      upload.waitForUploadResult();
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException(e.toString())
          .initCause(e);
    } catch (AmazonClientException e) {
      throw new IOException(String.format("saving output %s %s", mKey, e));
    } finally {
      if (!mBackupFile.delete()) {
        LOG.warn("Could not delete temporary cos file: {}", mBackupOutputStream);
      }
      super.close();
    }
    LOG.debug("OutputStream for key '{}' upload complete", mKey);
  }

}
