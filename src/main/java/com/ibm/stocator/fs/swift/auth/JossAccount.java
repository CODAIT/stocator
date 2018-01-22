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

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

import org.apache.http.impl.client.CloseableHttpClient;

/**
 *
 * The account model in Joss has session that contains token. When token expire,
 * token re-created automatically. In certain flows we need to get token value
 * and use it in the direct calls to the Swift API object stores. In this case
 * we need to cache the token and re-authenticate it only when 401 happens.
 *
 */
public class JossAccount {
  /*
   * Joss account object
   */
  private Account mAccount;
  /*
   * Joss configuration
   */
  private AccountConfig mAccountConfig;
  /*
   * Keystone region
   */
  private String mRegion;
  /*
   * use public or internal URL for Swift API object store
   */
  boolean mUsePublicURL;
  /*
   * Cached Access object. Will be renewed when token expire
   */
  private Access mAccess;
  private CloseableHttpClient httpclient = null;
  private static final Logger LOG = LoggerFactory.getLogger(JossAccount.class);

  /**
   * Constructor
   *
   * @param config
   *          Joss configuration
   * @param region
   *          Keystone region
   * @param usePublicURL
   *          use public or internal url
   * @param scm Swift connection manager
   */
  public JossAccount(AccountConfig config, String region, boolean usePublicURL,
      SwiftConnectionManager scm) {
    mAccountConfig = config;
    mRegion = region;
    mUsePublicURL = usePublicURL;
    mAccess = null;
    httpclient = scm.createHttpConnection();
  }

  /**
   * Creates account model
   */
  public void createAccount() {
    mAccount = new AccountFactory(mAccountConfig).setHttpClient(httpclient).createAccount();
    mAccess = mAccount.getAccess();
    if (mRegion != null) {
      mAccess.setPreferredRegion(mRegion);
    }
  }

  /**
   * Creates virtual account. Used for public containers
   */
  public void createDummyAccount() {
    mAccount = new DummyAccountFactory(mAccountConfig).setHttpClient(httpclient).createAccount();
    mAccess = mAccount.getAccess();
  }

  /**
   * Authenticates and renew the token
   */
  public void authenticate() {
    if (mAccount == null) {
      // Create account also performs authentication.
      createAccount();
    } else {
      mAccess = mAccount.authenticate();
      if (mRegion != null) {
        mAccess.setPreferredRegion(mRegion);
      }
    }
  }

  /**
   * Return current token
   *
   * @return cached token
   */
  public String getAuthToken() {
    return mAccess.getToken();
  }

  /**
   * Get authenticated URL
   *
   * @return access URL, public or internal
   */
  public String getAccessURL() {
    if (mUsePublicURL) {
      LOG.trace("Using public URL: " + mAccess.getPublicURL());
      return mAccess.getPublicURL();
    }
    LOG.trace("Using internal URL: " + mAccess.getInternalURL());
    return mAccess.getInternalURL();
  }

  /**
   * Get account
   *
   * @return Account
   */
  public Account getAccount() {
    if (mAccount == null) {
      createAccount();
    }
    return mAccount;
  }
}
