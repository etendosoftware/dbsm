/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.0  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SLU 
 * All portions are Copyright (C) 2009 Openbravo SLU 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */

package org.openbravo.ddlutils.util;

import java.util.ArrayList;

import org.apache.log4j.Logger;

public abstract class ValidateAPI {
  enum ValidationType {
    MODEL, DATA
  }

  ArrayList<String> errors;
  ArrayList<String> warnings;
  ValidationType valType;

  abstract public void execute();

  public ValidateAPI() {
    errors = new ArrayList<String>();
    warnings = new ArrayList<String>();
  }

  public boolean hasErrors() {
    return errors.size() > 0;
  }

  public boolean hasWarnings() {
    return warnings.size() > 0;
  }

  public void printErrors() {
    printErrors(null);
  }

  public void printErrors(Logger log) {
    if (errors.size() == 0)
      return;
    if (log != null) {
      log.error("\n");
      log.error("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      log.error("  Errors in API " + (valType.equals(ValidationType.DATA) ? "data" : "model")
          + " validation");
      log.error("+++++++++++++++++++++++++++++++++++++++++++++++++++");
    } else {
      System.out.println("\n");
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      System.out.println("  Errors in API "
          + (valType.equals(ValidationType.DATA) ? "data" : "model") + " validation");
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
    }
    for (String error : errors) {
      if (log != null)
        log.error("  " + error);
      else
        System.out.println("  " + error);
    }
    if (log != null)
      log.error("\n");
    else
      System.out.println("");
  }

  public void printWarnings() {
    printWarnings(null);
  }

  public void printWarnings(Logger log) {
    if (warnings.size() == 0)
      return;
    if (log != null) {
      log.warn("\n");
      log.warn("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      log.warn("  Warnings in API " + (valType.equals(ValidationType.DATA) ? "data" : "model")
          + " validation");
      log.warn("+++++++++++++++++++++++++++++++++++++++++++++++++++");
    } else {
      System.out.println("\n");
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
      System.out.println("  Warnings in API "
          + (valType.equals(ValidationType.DATA) ? "data" : "model") + " validation");
      System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");
    }
    for (String warn : warnings) {
      if (log != null)
        log.warn("  " + warn);
      else
        System.out.println("  " + warn);
    }
    if (log != null)
      log.warn("\n");
    else
      System.out.println("");
  }

}
