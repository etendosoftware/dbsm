/*
 *************************************************************************
 * The contents of this file are subject to the Openbravo  Public  License
 * Version  1.1  (the  "License"),  being   the  Mozilla   Public  License
 * Version 1.1  with a permitted attribution clause; you may not  use this
 * file except in compliance with the License. You  may  obtain  a copy of
 * the License at http://www.openbravo.com/legal/license.html 
 * Software distributed under the License  is  distributed  on  an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific  language  governing  rights  and  limitations
 * under the License. 
 * The Original Code is Openbravo ERP. 
 * The Initial Developer of the Original Code is Openbravo SLU 
 * All portions are Copyright (C) 2019 Openbravo SLU
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */
package org.openbravo.ddlutils.task;

import java.io.File;

/**
 * Java class that exposes some utilities methods that can be used by the different Java tasks.
 */
public class JavaTaskUtils {

  private static final String YES = "yes";
  private static final String ON = "on";

  private JavaTaskUtils() {
  }

  /**
   * Transforms the string representation of a boolean into its corresponding boolean value.
   * 
   * @param booleanValue
   *          The string representation of a boolean value.
   * 
   * @return {@code true} if the string argument is equal, ignoring case, to the strings "true",
   *         "yes" or "on". Otherwise, return {@code false}.
   */
  public static boolean getBooleanProperty(String booleanValue) {
    if (YES.equalsIgnoreCase(booleanValue) || ON.equalsIgnoreCase(booleanValue)) {
      return true;
    }
    return Boolean.parseBoolean(booleanValue);
  }

  /**
   * Transforms the string representation of an integer into its corresponding int value.
   * 
   * @param intValue
   *          The string representation of an integer value.
   * 
   * @return the integer value of the provided string representation or -1 if it is not possible to
   *         parse the value.
   */
  public static int getIntegerProperty(String intValue) {
    try {
      return Integer.parseInt(intValue);
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Returns a File whose path is passed as parameter.
   * 
   * @param file
   *          The string representation of the file path.
   * 
   * @return File whose path is passed as parameter.
   */
  public static File getFileProperty(String file) {
    return new File(file);
  }

}
