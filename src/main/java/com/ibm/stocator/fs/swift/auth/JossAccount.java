package com.ibm.stocator.fs.swift.auth;

import java.io.IOException;

import org.javaswift.joss.model.Access;
import org.javaswift.joss.model.Account;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.http.SwiftConnectionManager;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

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
  public JossAccount(AccountConfiguration config, String region, boolean usePublicURL,
      SwiftConnectionManager scm) {
    accountConfig = config;
    mRegion = region;
    mUsePublicURL = usePublicURL;
    mAccess = null;
    httpclient = scm.createHttpConnection();
  }

  /**
   * Creates account model
   */
  public void createAccount() {
//    mAccount = new AccountFactory(accountConfig).setHttpClient(httpclient).createAccount();
//    mAccess = mAccount.getAccess();
    if (mRegion != null) {
      mAccess.setPreferredRegion(mRegion);
    }
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
  public void authenticate() {
    HttpPost authRequest = new HttpPost(accountConfig.getAuthUrl());

    try {
      JSONObject object = new JSONObject();
      JSONObject auth = new JSONObject();
      JSONObject identity = new JSONObject();
      JSONArray method = new JSONArray();
      JSONObject password = new JSONObject();
      JSONObject user = new JSONObject();
      JSONObject scope = new JSONObject();
      JSONObject project = new JSONObject();

      project.put("id", accountConfig.getProjectId());
      scope.put("project", project);
      user.put("id", accountConfig.getUsername());
      user.put("password", accountConfig.getPassword());
      password.put("user", user);
      method.put("password");
      identity.put("methods", method);
      identity.put("password", password);
      auth.put("identity", identity);
      auth.put("scope", scope);
      object.put("auth", auth);

      System.out.println(object.toString());
      StringEntity body = new StringEntity(object.toString());

      authRequest.setEntity(body);
      authRequest.setHeader("Content-type", "application/json");
      ResponseHandler<String> responseHandler = new BasicResponseHandler();
      String response = httpclient.execute(authRequest, responseHandler);
      // TODO: handle the response

    } catch (JSONException je) {
      LOG.error("JSON error");

    } catch (IOException e) {
      LOG.error("Unable to authenticate. Please check credentials");
    }
    // TODO: handle preferred region

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
