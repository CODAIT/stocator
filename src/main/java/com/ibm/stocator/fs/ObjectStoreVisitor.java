/**
 * (C) Copyright IBM Corp. 2010, 2016
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

package com.ibm.stocator.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.swift.SwiftAPIClient;

/**
 * Pickup the correct object store implementation
 *
 */
public class ObjectStoreVisitor {
  public static IStoreClient getStoreClient(String nameSpace,
      URI fsuri, Configuration conf) throws IOException {
    if (nameSpace.equals(Constants.SWIFT)) {
      IStoreClient storeClient = new SwiftAPIClient(fsuri, conf);
      return storeClient;
    }
    throw new IOException("No object store for: " + nameSpace);
  }

}
