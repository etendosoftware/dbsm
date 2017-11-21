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
 * All portions are Copyright (C) 2017 Openbravo SLU 
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */
package org.openbravo.ddlutils.task;

import java.io.File;

import org.apache.ddlutils.task.VerbosityLevel;

public class AlterDatabaseJava {

  /**
   * @param args
   */
  public static void main(String[] args) {

    final AlterDatabaseDataAll ada = new AlterDatabaseDataAll();
    ada.setDriver(args[0]);
    ada.setUrl(args[1]);
    ada.setUser(args[2]);
    ada.setPassword(args[3]);
    ada.setExcludeobjects(args[4]);
    ada.setModel(new File(args[5]));
    // args[6] was 'filter' now unused
    ada.setInput(new File(args[7]));
    ada.setObject(args[8]);
    ada.setFailonerror(new Boolean(args[9]).booleanValue());
    ada.setVerbosity(new VerbosityLevel(args[10]));
    ada.setBasedir(args[11]);
    ada.setDirFilter(args[12]);
    ada.setDatadir(args[13]);
    ada.setDatafilter(args[14]);
    String force = args[15];
    if (force.equalsIgnoreCase("yes"))
      force = "true";
    ada.setForce(new Boolean(force).booleanValue());
    String strict = args[16];
    if (strict.equalsIgnoreCase("yes"))
      strict = "true";

    ada.setStrict(new Boolean(strict).booleanValue());

    if (args.length > 17) {
      ada.setForcedRecreation(args[17]);
    }
    if (args.length > 18) {
      ada.setExecuteModuleScripts("yes".equals(args[18]) || "true".equals(args[18])
          || "on".equals(args[18]));
    }

    if (args.length > 19) {
      int maxThreads;
      try {
        maxThreads = Integer.parseInt(args[19]);
      } catch (NumberFormatException e) {
        maxThreads = -1;
      }
      ada.setThreads(maxThreads);
    }

    if (args.length > 21) {
      ada.setSystemUser(args[20]);
      ada.setSystemPassword(args[21]);
    }

    ada.execute();
  }
}
