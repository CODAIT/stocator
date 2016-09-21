package com.ibm.stocator.fs.swift.auth;

public class AccountConfiguration {

  private boolean isPublic;
  private String tenant;
  private String password;
  private String username;
  private String projectId;
  private String region;
  private String authMethod;
  private String authUrl;

  public AccountConfiguration() {
    isPublic = false;
    tenant = null;
    password = null;
    username = null;
    projectId = null;
    region = null;
    authMethod = null;
    authUrl = null;
  }

  public boolean getPublic() {
    return isPublic;
  }

  public void setPublic() {
    isPublic = true;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenantName) {
    tenant = tenantName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String pw) {
    password = pw;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String user) {
    username = user;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String id) {
    projectId = id;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String preferredRegion) {
    region = preferredRegion;
  }

  public String getAuthMethod() {
    return authMethod;
  }

  public void setAuthMethod(String authenticationMethod) {
    authMethod = authenticationMethod;
  }

  public String getAuthUrl() {
    return authUrl;
  }

  public void setAuthUrl(String url) {
    authUrl = url;
  }

}
