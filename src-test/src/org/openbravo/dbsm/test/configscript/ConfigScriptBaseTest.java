/*
 ************************************************************************************
 * Copyright (C) 2016 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */
package org.openbravo.dbsm.test.configscript;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.ddlutils.model.Database;
import org.codehaus.jettison.json.JSONException;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;
import org.openbravo.dbsm.test.base.DbsmTest;

public abstract class ConfigScriptBaseTest extends DbsmTest {

  protected static final String MODEL_DIRECTORY = "configScripts/";
  protected static final String DATA_DIRECTORY = "data/configScripts";
  protected static final String EXPORT_DIRECTORY = "/tmp/export-test/";

  protected enum UpdateModelTask {
    installSource, updateDatabase
  }

  private UpdateModelTask task;

  public ConfigScriptBaseTest(String rdbms, String driver, String url, String sid, String user,
      String password, String name, UpdateModelTask task) throws FileNotFoundException, IOException {
    super(rdbms, driver, url, sid, user, password, name);
    this.task = task;
  }

  @Parameters(name = "DB: {6} - task: {7}")
  public static Collection<Object[]> parameters() throws IOException, JSONException {
    List<Object[]> configs = new ArrayList<Object[]>();

    for (String[] param : DbsmTest.params()) {
      for (UpdateModelTask task : UpdateModelTask.values()) {
        List<Object> p = new ArrayList<Object>(Arrays.asList(param));
        p.add(task);
        configs.add(p.toArray());
      }
    }
    return configs;
  }

  @Before
  public void executeOnlyInUpdateDatabaseTask() {
    // See issue https://issues.openbravo.com/view.php?id=34102
    // Currently some model changes are not supported by install.source
    // Meantime test classes extending this one will only consider update.database flow
    assumeThat("not executing install.source task", task, is(UpdateModelTask.updateDatabase));
  }

  protected Database exportModelChangesAndUpdateDatabase(String model) {
    cleanExportDirectory();
    resetDB();
    Database originalDB = createDatabase(model);
    Database modifiedDB = null;
    Database updatedDB = null;
    try {
      modifiedDB = (Database) originalDB.clone();
    } catch (CloneNotSupportedException ex) {
      log.error("Error cloning database", ex);
      return null;
    }
    // Create new changes
    doModelChanges(modifiedDB);
    // Export changes to configuration script
    exportToConfigScript(originalDB, modifiedDB, EXPORT_DIRECTORY);
    // Update database, applying the configuration script also
    updatedDB = updateDatabase(model, Arrays.asList(EXPORT_DIRECTORY + "configScript.xml"));
    return updatedDB;
  }

  protected void applyConfigurationScripts(String model, List<String> adTableNames,
      List<String> configScripts) {
    resetDB();
    boolean assertDBisCorrect = Boolean.TRUE;
    // Update database, applying the configuration script data changes also
    updateDatabase(model, DATA_DIRECTORY, adTableNames, assertDBisCorrect, configScripts);
  }

  private void cleanExportDirectory() {
    File exportTo = new File(EXPORT_DIRECTORY);
    if (exportTo.exists()) {
      exportTo.delete();
    }
    exportTo.mkdirs();
  }

  protected abstract void doModelChanges(Database database);

}
