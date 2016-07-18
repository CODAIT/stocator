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
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.IStoreClient;

/**
 * Pickup the correct object store implementation
 *
 */
public class ObjectStoreVisitor {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(ObjectStoreVisitor.class);

  /**
   * fs.stocator.scheme.list contains comma separated list of the provided
   * back-end storage drivers. If none provided or key is not present, then 'swift'
   * is the default value.
   *
   * For each provided scheme:
   *
   * fs.stocator.provided scheme.scheme - scheme value. If none provided, 'swift2d' is
   * the default value
   * fs.stocator.provided scheme.impl - scheme implementation class. If non provided
   * 'com.ibm.stocator.fs.swift.SwiftAPIClient' is the default value
   *
   * If the schema of fsuri equals one of the supported schema, then code will dynamically
   * load the implementation class.
   *
   * Example of the Stocator configuration:
   * {@code
   * <property>
   * <name>fs.stocator.scheme.list</name><value>swift,swift2d,test123</value>
   * </property>
   *
   * <property>
   * <name>fs.stocator.swift.scheme</name><value>swift</value>
   * </property>
   * <property>
   * <name>fs.stocator.swift.impl</name><value>com.ibm.stocator.fs.swift.SwiftAPIClient</value>
   * </property>
   *
   * <property>
   * <name>fs.stocator.swift2d.scheme</name><value>swift2d</value>
   * </property>
   * <property>
   * <name>fs.stocator.swift2d.impl</name><value>com.ibm.stocator.fs.swift.SwiftAPIClient</value>
   * </property>
   *
   * <property>
   * <name>fs.stocator.test123.scheme</name><value>test123</value>
   * </property>
   * <property>
   * <name>fs.stocator.test123.impl</name><value>some.class.Test123Client</value>
   * </property>
   * }
   *
   * Access Stocator via:
   * {@code
   * swift://<container>.<service>/object(s)
   * swift2d://<container>.<service>/object(s)
   * test123://<container>.<service>/object(s)
   * }
   *
   * @param fsuri uri to process
   * @param conf provided configuration
   * @return IStoreClient implementation
   * @throws IOException if error
   */
  public static IStoreClient getStoreClient(URI fsuri, Configuration conf) throws IOException {
    String fsSchema = fsuri.toString().substring(0, fsuri.toString().indexOf("://"));
    ClassLoader classLoader = ObjectStoreVisitor.class.getClassLoader();
    String[] supportedSchemas = conf.get("fs.stocator.scheme.list", Constants.SWIFT).split(",");
    for (String scheme: supportedSchemas) {
      String supportedScheme = conf.get("fs.stocator." + scheme.trim() + ".scheme",
          Constants.SWIFT2D);
      String implementation = conf.get("fs.stocator." + scheme.trim() + ".impl",
          "com.ibm.stocator.fs.swift.SwiftAPIClient");
      LOG.debug("Stocator schema space : {}, provided {}", fsSchema, supportedScheme);
      if (fsSchema.equals(supportedScheme)) {
        LOG.info("Stocator registered as {} for {}", fsSchema, fsuri.toString());
        try {
          Class<?> aClass = classLoader.loadClass(implementation);
          IStoreClient storeClient = (IStoreClient) aClass.getConstructor(fsuri.getClass(),
              conf.getClass()).newInstance(fsuri, conf);
          return storeClient;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException
            | ClassNotFoundException e) {
          LOG.error(e.getMessage());
          throw new IOException("No object store for: " + fsSchema);
        }
      }
    }
    throw new IOException("No object store for: " + fsSchema);
  }

}
