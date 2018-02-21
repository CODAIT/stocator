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

package com.ibm.stocator.fs.cos.auth;

import com.ibm.oauth.Token;
import com.ibm.oauth.TokenManager;

/**
 * <b>Simple example of a custom TokenManager implementation</b>. A User can
 * implement TokenManager to suit their requirements. In this custom
 * implementation the class will provide a randomly generated Token and refresh
 * if older than 60 seconds
 *
 */
public class CustomTokenManager implements TokenManager {

  private Token token;

  public CustomTokenManager(String userToken) {
    setToken(userToken);
  }

  @Override
  public String getToken() {
    return token.getAccess_token();
  }

  public void setToken(String userToken) {
    Token tokenTmp = new Token();
    tokenTmp.setAccess_token(userToken);
    tokenTmp.setExpiration(String.valueOf(System.currentTimeMillis() + 60 * 60 * 24 * 1000));
    token = tokenTmp;
  }

}
