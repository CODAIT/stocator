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

package com.ibm.stocator.fs.swift.auth;

import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.model.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dummy access is used to access public urls
 */
public class DummyAccess implements Access {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(DummyAccess.class);

  /*
   * Public access URL
   */
  private String accessURL;

  /**
   * Dummy Authentication
   *
   * @param url the access  url
   */
  public DummyAccess(String url) {
    LOG.debug("Dummy access");
    accessURL = url;
  }

  @Override
  public String getInternalURL() {
    return accessURL;
  }

  @Override
  public String getPublicURL() {
    return accessURL;
  }

  @Override
  public String getTempUrlPrefix(TempUrlHashPrefixSource arg0) {
    return null;
  }

  @Override
  public String getToken() {
    return "";
  }

  @Override
  public boolean isTenantSupplied() {
    return true;
  }

  @Override
  public void setPreferredRegion(String preferredRegion) {
  }

}
