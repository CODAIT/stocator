package com.ibm.stocator.fs.swift.auth;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 *
 * The account model in Joss has session that contains token.
 * When token expire, token re-created automatically.
 * In certain flows we need to get token value and use it in the direct
 * calls to the Swift API object stores.
 * In this case we need to cache the token and re-authenticate it only when 401 happens.
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
  /*
   * HttpClient to be used for requests
   */
  private HttpClient client;

  /**
   * Constructor
   *
   * @param config Joss configuration
   * @param region Keystone region
   * @param usePublicURL use public or internal url
   */
  public JossAccount(AccountConfig config, String region, boolean usePublicURL) {
    mAccountConfig = config;
    mRegion = region;
    mUsePublicURL = usePublicURL;
    mAccess = null;
    client = initHttpClient();
  }

  /**
   * Creates account model
   */
  public void createAccount() {
    mAccount = new AccountFactory(mAccountConfig).setHttpClient(client).createAccount();
  }

  /**
   * Creates virtual account. Used for public containers
   */
  public void createDummyAccount() {
    mAccount = new DummyAccountFactory(mAccountConfig).setHttpClient(client).createAccount();
  }

  /**
   * Authenticates and renew the token
   */
  public void authenticate() {
    if (mAccount == null) {
      createAccount();
    }
    mAccess = mAccount.authenticate();
    if (mRegion != null) {
      mAccess.setPreferredRegion(mRegion);
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
      return mAccess.getPublicURL();
    }
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

  /**
   * Creates a thread safe HttpClient to use for requests
   */
  private HttpClient initHttpClient() {
    PoolingHttpClientConnectionManager manager = new PoolingHttpClientConnectionManager();
    manager.setDefaultMaxPerRoute(15);
    manager.setMaxTotal(15);

    return HttpClients.custom().setConnectionManager(manager).build();

  }

  /**
   * Get HttpClient
   * @return client The client being used
   */
  public HttpClient getHttpClient() {
    return client;
  }
}
