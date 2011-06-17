package org.apache.ddlutils.alteration;

public class VersionInfo implements Change {
  String version;

  public VersionInfo() {

  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

}
