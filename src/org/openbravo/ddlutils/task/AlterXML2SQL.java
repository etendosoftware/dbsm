/*
 ************************************************************************************
 * Copyright (C) 2001-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.openbravo.ddlutils.task;

import java.io.File;

import org.apache.ddlutils.platform.FileBatchEvaluator;
import org.openbravo.ddlutils.process.DBUpdater;

/** Generates a SQL file with the statements that update.database would execute */
public class AlterXML2SQL extends AlterDatabaseDataAll {
  private File output;

  @Override
  protected void doExecute() {
    getLog().info("Generating update.database script in " + output);
    super.doExecute();
  }

  @Override
  protected DBUpdater getDBUpater() {
    DBUpdater updater = super.getDBUpater();

    updater.getPlatform().setBatchEvaluator(new FileBatchEvaluator(output));
    updater.setUpdateCheckSums(false);
    updater.setUpdateModuleInstallTables(false);
    updater.setExecuteModuleScripts(false);

    return updater;
  }

  /** Sets file to generate script */
  public void setOutput(File output) {
    this.output = output;
  }
}
