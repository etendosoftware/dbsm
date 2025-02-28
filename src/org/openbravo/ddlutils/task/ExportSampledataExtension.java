/*
 ************************************************************************************
 * Copyright (C) 2023 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

public class ExportSampledataExtension extends ExportSampledata {
  private String dataSetName = null;

  public void setDataSetName(String dataSetName) {
    this.dataSetName = dataSetName;
  }

  @Override
  protected String getDataSet() {
    return this.dataSetName;
  }

  @Override
  protected boolean overwriteDataSetTableWhereClauseWithClientFilter() {
    return false;
  }

  public static void main(String[] args) {
    createExportSampledata(args).execute();
  }

  private static ExportSampledata createExportSampledata(String[] args) {
    ExportSampledataExtension exportSampledata = new ExportSampledataExtension();
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
    exportSampledata.setDataSetName(args[10]);
    return exportSampledata;
  }
}
