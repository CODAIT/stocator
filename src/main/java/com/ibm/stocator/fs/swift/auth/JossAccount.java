package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Access;

import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpResponse;
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
   * Swift configuration
   */
  private AccountConfiguration accountConfig;
  /*
   * Keystone region
   */
  private String mRegion;
  /*
   * use public or internal URL for Swift API object store
   */
  boolean mUsePublicURL;
  /*
   * Contains token information
   */
  private AuthenticationInfo authenticationInfo;
  private CloseableHttpClient httpclient = null;
  private String accessUrl;
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
  public JossAccount(AccountConfiguration config, String region, boolean usePublicURL,
      SwiftConnectionManager scm) {
    accountConfig = config;
    mRegion = region;
    mUsePublicURL = usePublicURL;
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
      authenticationInfo = new SwiftV3AuthInfo();
    } else if (authMethod.equals("keystone")) {
      authRequest = new KeystoneV2AuthenticationRequest(accountConfig);
      authenticationInfo = new SwiftV2AuthInfo();
    } else {
      authRequest = new SwiftAuthenticationRequest(accountConfig);
      authenticationInfo = new SwiftV1AuthInfo();
    }

    try {
      HttpResponse response = httpclient.execute(authRequest);
      authenticationInfo.parseResponse(response);

      if (response.getStatusLine().getStatusCode() == 201) {
        System.out.println("Auth success");
      }

    } catch (IOException e) {
      LOG.error("Unable to authenticate. Please check credentials");
    }
    getAccessURL();
    // TODO(djalova): handle preferred region & improve error message

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

    if (accessUrl == null) {
      try {
        queryAccessUrl();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return accessUrl;
  }

  private String queryAccessUrl() throws IOException {

    String url = accountConfig.getAuthUrl().substring(0,
            accountConfig.getAuthUrl().lastIndexOf("/")) + "/catalog";
    HttpGet getEndpoints = new HttpGet(url);
    getEndpoints.addHeader("X-Auth-Token", getAuthToken());
    try {
      HttpResponse response = httpclient.execute(getEndpoints);
      ResponseHandler handler = new BasicResponseHandler();
      JSONObject jsonResponse = new JSONObject(handler.handleResponse(response).toString());
      JSONArray catalog = jsonResponse.getJSONArray("catalog");
      JSONArray swiftEndpoints = null;
      for (int i = 0; i < catalog.length() && swiftEndpoints == null; i++) {
        JSONObject service = catalog.getJSONObject(i);
        if (service.getString("name").equals("swift")) {
          swiftEndpoints = service.getJSONArray("endpoints");
        }
      }

      if (swiftEndpoints == null) {
        // Exception
      }

      String isPublic = accountConfig.getPublic() ? "public" : "internal";
      for (int i = 0;i < swiftEndpoints.length(); i++) {
        JSONObject endpoint = swiftEndpoints.getJSONObject(i);
        if (endpoint.get("interface").equals(isPublic)) {
          if (accountConfig.getRegion() != null) {
            // Return URL that matches region and interface
            if (accountConfig.getRegion().equals(endpoint.getString("region"))) {
              accessUrl = endpoint.getString("url");
              LOG.trace("Using {} {} URL: {}", isPublic, accountConfig.getRegion(), accessUrl);
            }
          } else {
            // No region preference,return any URL
            accessUrl = endpoint.getString("url");
            LOG.trace("Using {} URL: {}", isPublic, accessUrl);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (accessUrl == null) {
      throw new IOException("Unable to get url with provided public and a region configs");
    }

    return accessUrl;
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
