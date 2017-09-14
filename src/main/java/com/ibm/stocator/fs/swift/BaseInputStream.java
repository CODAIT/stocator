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

  protected final void conditionalAbort() {
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
    conditionalAbort();
    return in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    conditionalAbort();
    return in.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    conditionalAbort();
    return in.skip(n);
  }

  @Override
  public int available() throws IOException {
    conditionalAbort();
    return in.available();
  }

  @Override
  public void close() throws IOException {
    in.close();
    conditionalAbort();
  }

  @Override
  public synchronized void mark(int readlimit) {
    conditionalAbort();
    in.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    conditionalAbort();
    in.reset();
  }

  @Override
  public boolean markSupported() {
    conditionalAbort();
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
