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
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.client.factory.TempUrlHashPrefixSource;
import org.javaswift.joss.client.mock.ClientMock;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Client;
import org.apache.http.client.HttpClient;

public class DummyAccountFactory {

  private final AccountConfig mConfig;

  private HttpClient mHttpClient;

  public DummyAccountFactory() {
    this(new AccountConfig());
  }

  public DummyAccountFactory(AccountConfig config) {
    mConfig = config;
  }

  public Account createAccount() {
    final Client client;
    if (mConfig.isMock()) {
      client = createClientMock();
    } else {
      client = createClientImpl();
    }
    return client.authenticate();
  }

  public Client createClientMock() {
    return new ClientMock(mConfig);
  }

  public Client createClientImpl() {
    return new DummyClientImpl(mConfig).setHttpClient(mHttpClient);
  }

  public DummyAccountFactory setTenantName(String tenantName) {
    mConfig.setTenantName(tenantName);
    return this;
  }

  public DummyAccountFactory setTenantId(String tenantId) {
    mConfig.setTenantId(tenantId);
    return this;
  }

  public DummyAccountFactory setUsername(String username) {
    mConfig.setUsername(username);
    return this;
  }

  public DummyAccountFactory setPassword(String password) {
    mConfig.setPassword(password);
    return this;
  }

  public DummyAccountFactory setAuthUrl(String authUrl) {
    mConfig.setAuthUrl(authUrl);
    return this;
  }

  public DummyAccountFactory setMock(boolean mock) {
    mConfig.setMock(mock);
    return this;
  }

  public DummyAccountFactory setPublicHost(String publicHost) {
    mConfig.setPublicHost(publicHost);
    return this;
  }

  public DummyAccountFactory setPrivateHost(String privateHost) {
    mConfig.setPrivateHost(privateHost);
    return this;
  }

  public DummyAccountFactory setMockMillisDelay(int mockMillisDelay) {
    mConfig.setMockMillisDelay(mockMillisDelay);
    return this;
  }

  public DummyAccountFactory setAllowReauthenticate(boolean allowReauthenticate) {
    mConfig.setAllowReauthenticate(allowReauthenticate);
    return this;
  }

  public DummyAccountFactory setAllowCaching(boolean allowCaching) {
    mConfig.setAllowCaching(allowCaching);
    return this;
  }

  public DummyAccountFactory setAllowContainerCaching(boolean allowContainerCaching) {
    mConfig.setAllowContainerCaching(allowContainerCaching);
    return this;
  }

  public DummyAccountFactory setMockAllowObjectDeleter(boolean mockAllowObjectDeleter) {
    mConfig.setMockAllowObjectDeleter(mockAllowObjectDeleter);
    return this;
  }

  public DummyAccountFactory setMockAllowEveryone(boolean mockAllowEveryone) {
    mConfig.setMockAllowEveryone(mockAllowEveryone);
    return this;
  }

  public DummyAccountFactory setMockOnFileObjectStore(String mockOnFileObjectStore) {
    mConfig.setMockOnFileObjectStore(mockOnFileObjectStore);
    return this;
  }

  public DummyAccountFactory setMockOnFileObjectStoreIsAbsolutePath(boolean absolutePath) {
    mConfig.setMockOnFileObjectStoreIsAbsolutePath(absolutePath);
    return this;
  }

  public DummyAccountFactory setSocketTimeout(int socketTimeout) {
    mConfig.setSocketTimeout(socketTimeout);
    return this;
  }

  public DummyAccountFactory setPreferredRegion(String preferredRegion) {
    mConfig.setPreferredRegion(preferredRegion);
    return this;
  }

  public DummyAccountFactory setHashPassword(String hashPassword) {
    mConfig.setHashPassword(hashPassword);
    return this;
  }

  public DummyAccountFactory setTempUrlHashPrefixSource(TempUrlHashPrefixSource source) {
    mConfig.setTempUrlHashPrefixSource(source);
    return this;
  }

  public DummyAccountFactory setAuthenticationMethod(AuthenticationMethod authenticationMethod) {
    mConfig.setAuthenticationMethod(authenticationMethod);
    return this;
  }

  public DummyAccountFactory setDelimiter(Character delimiter) {
    mConfig.setDelimiter(delimiter);
    return this;
  }

  public DummyAccountFactory setHttpClient(HttpClient httpClient) {
    mHttpClient = httpClient;
    return this;
  }

}
