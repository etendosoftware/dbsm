package org.openbravo.ddlutils.util;

import java.util.Vector;

import org.apache.ddlutils.platform.ExcludeFilter;

public class ModuleRow {
  public Vector<String> prefixes;
  public Vector<ExceptionRow> exceptions;
  public Vector<ExceptionRow> othersexceptions;
  public String name;
  public String isInDevelopment;
  public String dir;
  public String idMod;
  public ExcludeFilter filter;
  public String type;
}