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

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.client.methods.HttpRequestBase;

import com.ibm.stocator.fs.common.Utils;

public class SwiftInputStreamWrapper extends BaseInputStream {

  /*
   * Http request
   */
  private final HttpRequestBase httpRequest;

  /*
   * no more left to read
   */
  private boolean finish;

  public SwiftInputStreamWrapper(InputStream in, HttpRequestBase httpRequestT) {
    super(in);
    httpRequest = httpRequestT;
  }

  @Override
  public void abort() {
    doAbort();
  }

  private void doAbort() {
    if (httpRequest != null) {
      httpRequest.abort();
    }
    Utils.closeWithoutException(in);
  }

  public HttpRequestBase getHttpRequest() {
    return httpRequest;
  }

  @Override
  public int available() throws IOException {
    int estimate = super.available();
    return estimate == 0 ? 1 : estimate;
  }

  @Override
  public int read() throws IOException {
    int value = super.read();
    if (value == -1) {
      finish = true;
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int value = super.read(b, off, len);
    if (value == -1) {
      finish = true;
    }
    return value;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    finish = false;
  }

  @Override
  public void close() throws IOException {
    if (finish) {
      super.close();
    } else {
      doAbort();
    }
  }
}
