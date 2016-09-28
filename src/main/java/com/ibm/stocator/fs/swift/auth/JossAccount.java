package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;

import org.javaswift.joss.model.Account;

import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Swift configuration
   */
  private AccountConfiguration accountConfig;
  /*
   * Contains token information
   */
  private AuthenticationInfo authenticationInfo;
  private CloseableHttpClient httpclient = null;
  private static final Logger LOG = LoggerFactory.getLogger(JossAccount.class);

  /**
   * Constructor
   *
   * @param config
   *          Joss configuration
   * @param usePublicURL
   *          use public or internal url
   * @param scm Swift connection manager
   */
  public JossAccount(AccountConfiguration config, SwiftConnectionManager scm) {
    accountConfig = config;
    httpclient = scm.createHttpConnection();
  }

  /**
   * Creates account model
   */
  public void createAccount() {
//    mAccount = new AccountFactory(accountConfig).setHttpClient(httpclient).createAccount();
//    mAccess = mAccount.getAccess();

  }

  /**
   * Creates virtual account. Used for public containers
   */
  public void createDummyAccount() {
    // mAccount = new DummyAccountFactory(accountConfig).setHttpClient(httpclient).createAccount();
  }

  /**
   * Authenticates and renew the token
   */
  public void authenticate() throws IOException {
    AuthenticationRequest authRequest;
    String authMethod = accountConfig.getAuthMethod();

    if (authMethod.equals("keystoneV3")) {
      authRequest = new KeystoneV3AuthenticationRequest(accountConfig);
      authenticationInfo = new SwiftV3AuthInfo(accountConfig);
    } else if (authMethod.equals("keystone")) {
      authRequest = new KeystoneV2AuthenticationRequest(accountConfig);
      authenticationInfo = new SwiftV2AuthInfo(accountConfig);
    } else {
      authRequest = new SwiftAuthenticationRequest(accountConfig);
      authenticationInfo = new SwiftV1AuthInfo();
    }

    try {
      HttpResponse response = httpclient.execute(authRequest);

      int statusCode = response.getStatusLine().getStatusCode();
      if ( statusCode == 200 || statusCode == 201 ) {
        System.out.println("Auth success");
        authenticationInfo.parseResponse(response);
      } else {
        LOG.error("Authentication request failed for reason: {}",
                response.getStatusLine().getReasonPhrase());
      }

    } catch (IOException e) {
      LOG.error("Unable to authenticate. Please check credentials");
    }
  }

  /**
   * Return current token
   *
   * @return cached token
   */
  public String getAuthToken() {
    return authenticationInfo.getToken();
  }

  /**
   * Get authenticated URL
   *
   * @return access URL, public or internal
   */
  public String getAccessURL() {

    return authenticationInfo.getAccessUrl();

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
