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

/**
 * Java class that exposes its main method for invoking the {@link ExportDatabase} task.
 */
public class ExportDatabaseJava {

  /**
   * @param args
   *          the arguments required to initialize the {@link ExportDatabase} task
   */
  public static void main(String[] args) {
    createExportDatabase(args).execute();
  }

  private static ExportDatabase createExportDatabase(String[] args) {
    ExportDatabase exportDatabase = new ExportDatabase();
    exportDatabase.setDriver(args[0]);
    exportDatabase.setUrl(args[1]);
    exportDatabase.setUser(args[2]);
    exportDatabase.setPassword(args[3]);
    exportDatabase.setModel(JavaTaskUtils.getFileProperty(args[4]));
    exportDatabase.setModuledir(JavaTaskUtils.getFileProperty(args[5]));
    exportDatabase.setOutput(JavaTaskUtils.getFileProperty(args[6]));
    exportDatabase.setUserId(args[7]);
    exportDatabase.setAdminMode(JavaTaskUtils.getBooleanProperty(args[8]));
    exportDatabase.setPropertiesFile(args[9]);
    exportDatabase.setForce(JavaTaskUtils.getBooleanProperty(args[10]));
    exportDatabase.setValidateModel(JavaTaskUtils.getBooleanProperty(args[11]));
    exportDatabase.setTestAPI(JavaTaskUtils.getBooleanProperty(args[12]));
    exportDatabase.setDatasetList(args[13]);
    exportDatabase.setRd(JavaTaskUtils.getBooleanProperty(args[14]));
    exportDatabase.setCheckTranslationConsistency(JavaTaskUtils.getBooleanProperty(args[15]));
    exportDatabase.setThreads(JavaTaskUtils.getIntegerProperty(args[16]));
    return exportDatabase;
  }
}
