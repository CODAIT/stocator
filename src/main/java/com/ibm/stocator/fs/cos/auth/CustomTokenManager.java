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
    Token tokenTmp = new Token();
    tokenTmp.setAccess_token(userToken);
    tokenTmp.setExpiration(String.valueOf(System.currentTimeMillis() + 60 * 60 * 24 * 1000));
    token = tokenTmp;
  }

  @Override
  public String getToken() {
    return token.getAccess_token();
  }

}
