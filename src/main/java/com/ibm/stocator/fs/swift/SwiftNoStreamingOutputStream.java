/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.swift;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.LocalDirAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.auth.JossAccount;
import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

/**
 *
 * Wraps OutputStream
 * This class is not thread-safe
 *
 */
public class SwiftNoStreamingOutputStream extends OutputStream {
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftNoStreamingOutputStream.class);
  /*
   * Access url
   */
  private URL mUrl;

  private JossAccount mAccount;
  /*
   * Output stream
   */
  private OutputStream mBackupOutputStream;
  /*
   * Temporal output file (output stream is buffered by it)
   */
  private final File mBackupFile;
  /*
   * Closed bit. Updates must be in a synchronized block to guarantee an atomic
   * check and set
   */
  private final SwiftConnectionManager scm;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Map<String, String> metadata;
  private final String contentType;

  /**
   * Default constructor
   *
   * @param account JOSS account object
   * @param url URL connection
   * @param contentTypeT content type
   * @param metadataT input metadata
   * @param connectionManager SwiftConnectionManager
   * @param fsT SwiftAPIClient
   * @throws IOException if error
   */
  public SwiftNoStreamingOutputStream(JossAccount account, URL url, final String contentTypeT,
                           Map<String, String> metadataT, SwiftConnectionManager connectionManager,
                           SwiftAPIClient fsT)
          throws IOException {
    LOG.debug("SwiftNoStreamingOutputStream constructor entry for {}", url.toString());
    mUrl = url;
    contentType = contentTypeT;
    mAccount = account;
    scm = connectionManager;
    metadata = metadataT;
    try {
      mBackupFile = fsT.createTmpFileForWrite("output-",
          LocalDirAllocator.SIZE_UNKNOWN);

      LOG.debug("OutputStream for key '{}' writing to tempfile: {}", mUrl.toString(), mBackupFile);
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
    LOG.debug("OutputStream for key '{}' closed. Now beginning upload", mUrl.toString());
    InputStream is = new FileInputStream(mBackupFile);
    try {
      int resp = SwiftAPIDirect.putObject(mUrl.toString(), mAccount, is, scm, metadata,
          mBackupFile.length(), contentType);
    } catch (Exception e) {
      throw new IOException(String.format("saving output %s %s", mUrl.toString(), e));
    } finally {
      if (!mBackupFile.delete()) {
        LOG.warn("Could not delete temporary cos file: {}", mBackupOutputStream);
      }
      is.close();
      super.close();
    }
    LOG.debug("OutputStream for key '{}' upload complete", mUrl.toString());
  }
}
