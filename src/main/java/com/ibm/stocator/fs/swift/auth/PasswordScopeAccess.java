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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extends JOSS Access Interface to support Keystone V3 API
 */
public class PasswordScopeAccess implements Access {

  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(PasswordScopeAccess.class);

  /*
   * Authentication token
   */
  private String manualToken;

  /*
   * Internal access URL
   */
  private String internalURL;

  /*
   * Public access URL
   */
  private String publicURL;

  /*
   * Keystone preferred region
   */
  private String prefferedRegion;

  /**
   * Parse Keystone V3 API
   * Password Scoped Authentication
   *
   * @param jsonResponse response from the Keystone
   * @param manualToken1 manual token if present
   * @param prefRegion Keystone preffered region
   */
  public PasswordScopeAccess(final JSONObject jsonResponse,
                             final String manualToken1,
                             final String prefRegion) {
    prefferedRegion = prefRegion;
    manualToken = manualToken1;
    final JSONObject token = (JSONObject) jsonResponse.get("token");
    final JSONArray catalog = (JSONArray) token.get("catalog");
    for (Object obj: catalog) {
      final JSONObject jObj = (JSONObject) obj;
      final String name = (String) jObj.get("name");
      final String type = (String) jObj.get("type");
      if (name.equals("swift") && type.equals("object-store")) {
        final JSONArray endPoints = (JSONArray) jObj.get("endpoints");
        for (Object endPointObj: endPoints) {
          final JSONObject endPoint = (JSONObject) endPointObj;
          final String region = (String) endPoint.get("region");
          if (region.equals(prefferedRegion)) {
            final String interfaceType = (String) endPoint.get("interface");
            if (interfaceType.equals("public")) {
              publicURL = (String) endPoint.get("url");
            } else if (interfaceType.equals("internal")) {
              internalURL = (String) endPoint.get("url");
            }
          }
        }
      }
    }
  }

  @Override
  public String getInternalURL() {
    return internalURL;
  }

  @Override
  public String getPublicURL() {
    return publicURL;
  }

  @Override
  public String getTempUrlPrefix(TempUrlHashPrefixSource arg0) {
    return null;
  }

  @Override
  public String getToken() {
    return manualToken;
  }

  @Override
  public boolean isTenantSupplied() {
    return true;
  }

  @Override
  public void setPreferredRegion(String arg0) {
    prefferedRegion = arg0;
  }
}
