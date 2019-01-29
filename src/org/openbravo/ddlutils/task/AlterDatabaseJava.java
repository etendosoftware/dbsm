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
 * All portions are Copyright (C) 2017-2019 Openbravo SLU
 * All Rights Reserved. 
 * Contributor(s):  ______________________________________.
 ************************************************************************
 */
package org.openbravo.ddlutils.task;

import java.io.File;

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
    ada.setFailonerror(JavaTaskUtils.getBooleanProperty(args[9]));
    ada.setBasedir(args[10]);
    ada.setDirFilter(args[11]);
    ada.setDatadir(args[12]);
    ada.setDatafilter(args[13]);
    ada.setForce(JavaTaskUtils.getBooleanProperty(args[14]));
    ada.setStrict(JavaTaskUtils.getBooleanProperty(args[15]));

    if (args.length > 16) {
      ada.setForcedRecreation(args[16]);
    }

    if (args.length > 17) {
      ada.setExecuteModuleScripts(JavaTaskUtils.getBooleanProperty(args[17]));
    }

    if (args.length > 18) {
      ada.setThreads(JavaTaskUtils.getIntegerProperty(args[18]));
    }

    if (args.length > 20) {
      ada.setSystemUser(args[19]);
      ada.setSystemPassword(args[20]);
    }

    ada.execute();
  }
}
