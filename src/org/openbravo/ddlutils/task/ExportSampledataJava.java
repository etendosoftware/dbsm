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
 * Java class that exposes its main method for invoking the {@link ExportSampledata} task.
 */
public class ExportSampledataJava {

  /**
   * @param args
   *          the arguments required to initialize the {@link ExportSampledata} task
   */
  public static void main(String[] args) {
    createExportSampledata(args).execute();
  }

  private static ExportSampledata createExportSampledata(String[] args) {
    ExportSampledata exportSampledata = new ExportSampledata();
    exportSampledata.setDriver(args[0]);
    exportSampledata.setUrl(args[1]);
    exportSampledata.setUser(args[2]);
    exportSampledata.setPassword(args[3]);
    exportSampledata.setRdbms(args[4]);
    exportSampledata.setBasedir(args[5]);
    exportSampledata.setClient(args[6]);
    exportSampledata.setModule(args[7]);
    exportSampledata.setExportFormat(args[8]);
    exportSampledata.setThreads(JavaTaskUtils.getIntegerProperty(args[9]));
    return exportSampledata;
  }
}
