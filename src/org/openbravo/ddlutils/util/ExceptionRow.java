package org.openbravo.ddlutils.util;

public class ExceptionRow implements Cloneable {
  public String name1;
  public String name2;
  public String type;

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      // Will never happen
      return null;
    }
  }
}
