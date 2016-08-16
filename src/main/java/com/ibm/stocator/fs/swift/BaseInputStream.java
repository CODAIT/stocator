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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.ibm.stocator.fs.common.Releasable;
import com.ibm.stocator.fs.common.Utils;
import com.ibm.stocator.fs.common.exception.AbortedException;

public class BaseInputStream extends FilterInputStream implements Releasable {
  protected BaseInputStream(InputStream in) {
    super(in);
  }

  protected final void abortIfNeeded() {
    if (Utils.shouldAbort()) {
      abort();
      throw new AbortedException();
    }
  }

  /**
   * abort
   */
  protected void abort() {
  }

  @Override
  public int read() throws IOException {
    abortIfNeeded();
    return in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    abortIfNeeded();
    return in.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    abortIfNeeded();
    return in.skip(n);
  }

  @Override
  public int available() throws IOException {
    abortIfNeeded();
    return in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
    abortIfNeeded();
  }

  @Override
  public synchronized void mark(int readlimit) {
    abortIfNeeded();
    in.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    abortIfNeeded();
    in.reset();
  }

  @Override
  public boolean markSupported() {
    abortIfNeeded();
    return in.markSupported();
  }

  @Override
  public void release() {
    Utils.closeWithoutException(this);
    if (in instanceof Releasable) {
      Releasable r = (Releasable) in;
      r.release();
    }
  }
}
