/*
 ************************************************************************************
 * Copyright (C) 2016-2017 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.configscript;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.ddlutils.alteration.Change;
import org.apache.ddlutils.model.Database;
import org.openbravo.dbsm.test.base.DbsmTest;

public abstract class ConfigScriptBaseTest extends DbsmTest {

  protected static final String MODEL_DIRECTORY = "configScripts/";
  protected static final String DATA_DIRECTORY = "data/configScripts";
  protected static final String EXPORT_DIRECTORY = "/tmp/export-test/";

  public ConfigScriptBaseTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
  }

  protected Database exportModelChangesAndUpdateDatabase(String model, List<String> configScripts) {
    return exportModelChangesAndUpdateDatabase(model, null, configScripts, null);
  }

  protected Database exportModelChangesAndUpdateDatabase(String model, List<String> adTableNames,
      List<String> configScripts, String dataDir) {
    cleanExportDirectory();
    resetDB();
    Database originalDB = createDatabase(model, configScripts);
    Database modifiedDB = null;
    Database updatedDB = null;
    try {
      modifiedDB = (Database) originalDB.clone();
    } catch (CloneNotSupportedException ex) {
      log.error("Error cloning database", ex);
      return null;
    }

    // Export changes to configuration script
    exportToConfigScript(originalDB, modifiedDB, EXPORT_DIRECTORY);

    // Update database, applying the configuration script also..maybe false?
    updatedDB = updateDatabase(model, dataDir, adTableNames, true, configScripts);
    return updatedDB;
  }

  protected void applyConfigurationScripts(String model, List<String> adTableNames,
      List<String> configScripts) {
    resetDB();
    boolean assertDBisCorrect = Boolean.TRUE;
    // Update database, applying the configuration script data changes also
    updateDatabase(model, DATA_DIRECTORY, adTableNames, assertDBisCorrect, configScripts);
  }

  protected Database createDatabaseAndApplyConfigurationScript(String model,
      List<String> configScripts) {
    resetDB();
    Database originalDB = createDatabase(model, configScripts);
    return originalDB;
  }

  protected void applyConfigurationScript(String model, List<String> adTableNames,
      String configScript) {
    resetDB();
    Database database = updateDatabase(model, DATA_DIRECTORY, adTableNames);
    Vector<Change> changes = readConfigScript(configScript);
    if (changes == null) {
      log.info("No changes retrieved from Configuration Script: " + configScript);
    } else {
      getPlatform().applyConfigScript(database, changes);
    }
  }

  protected List<String> getRowValues(String rowId, String testTabe, Set<String> dataChanges) {
    List<String> values = new ArrayList<String>();
    try {
      Row row = getRowValues(testTabe, rowId);
      for (String column : dataChanges) {
        values.add(getColumnValue(row, column));
      }
    } catch (SQLException sqlex) {
      log.error("Error retrieving row", sqlex);
    }
    return values;
  }

  private String getColumnValue(Row row, String columnName) {
    if (getRdbms() == Rdbms.ORA) {
      return row.getValue(columnName.toUpperCase());
    }
    return row.getValue(columnName.toLowerCase());
  }

  private void cleanExportDirectory() {
    File exportTo = new File(EXPORT_DIRECTORY);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();
  }
}
