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

package com.ibm.stocator.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;
import com.ibm.stocator.fs.swift.SwiftAPIClient;

/**
 * Pickup the correct object store implementation
 *
 */
public class ObjectStoreVisitor {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStoreVisitor.class);

  public static IStoreClient getStoreClient(String nameSpace,
      URI fsuri, Configuration conf) throws IOException {
    String providedNameSpace = conf.get("fs.swift.schema", Constants.SWIFT2D);
    LOG.debug("Stocator name space : {}, provided {}", nameSpace, providedNameSpace);
    if (nameSpace.equals(providedNameSpace)) {
      LOG.info("Swift API registered as {} for {}", nameSpace, fsuri.toString());
      IStoreClient storeClient = new SwiftAPIClient(fsuri, conf);
      return storeClient;
    }
    throw new IOException("No object store for: " + nameSpace);
  }

}
