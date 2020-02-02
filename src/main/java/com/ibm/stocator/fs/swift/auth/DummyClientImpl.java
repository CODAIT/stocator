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

import org.javaswift.joss.client.core.AbstractClient;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.impl.AccountImpl;
import org.javaswift.joss.command.impl.factory.AuthenticationCommandFactoryImpl;
import org.javaswift.joss.command.shared.factory.AuthenticationCommandFactory;
import org.javaswift.joss.command.shared.identity.AuthenticationCommand;
import org.javaswift.joss.model.Access;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyClientImpl extends AbstractClient<AccountImpl> {

  public static final Logger LOG = LoggerFactory.getLogger(DummyClientImpl.class);

  private HttpClient mHttpClient;

  public DummyClientImpl(AccountConfig accountConfig) {
    super(accountConfig);
  }

  @Override
  public AccountImpl authenticate() {
    AccountImpl account = createAccount();
    return account;

  }

  @Override
  protected void logSettings() {
    LOG.info("JOSS / Creating real account instance");
    LOG.info("JOSS / * Allow caching: " + accountConfig.isAllowCaching());
  }

  @Override
  protected AuthenticationCommandFactory createFactory() {
    return new AuthenticationCommandFactoryImpl();
  }

  @Override
  protected AccountImpl createAccount() {
    AuthenticationCommand command = new AuthenticationCommand() {

      @Override
      public Access call() {
        return new DummyAccess(accountConfig.getAuthUrl());
      }

      @Override
      public String getUrl() {
        return "abc";
      }
    };
    Access access = new DummyAccess(accountConfig.getAuthUrl()); // command.call();
    LOG.info("JOSS / Successfully authenticated");
    // access.setPreferredRegion(accountConfig.getPreferredRegion());
    LOG.info("JOSS / Applying preferred region: "
        + (accountConfig.getPreferredRegion() == null ? "none"
            : accountConfig.getPreferredRegion()));
    LOG.info("JOSS / Using TempURL hash prefix source: "
            + accountConfig.getTempUrlHashPrefixSource());
    return new AccountImpl(command, mHttpClient, access, accountConfig.isAllowCaching(),
        accountConfig.getTempUrlHashPrefixSource(), accountConfig.getDelimiter());
  }

  public DummyClientImpl setHttpClient(HttpClient httpClient) {
    mHttpClient = httpClient;
    return this;
  }
}
